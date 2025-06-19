'''
Contains code for running the orchestrator, defines all the API functionalities used by the users and the nodes to interact with the orchestrator.

'''

import grpc
import logging
import hashlib
from functools import wraps
from concurrent import futures

from jose import JWTError, jwt
from datetime import datetime, timedelta 
from dateutil.parser import parse as datetimeParse 
from google.protobuf.json_format import MessageToDict

from orchestrator.datastore import LeotestDatastoreMongo

import common.leotest_pb2_grpc as pb2_grpc
import common.leotest_pb2 as pb2

from common.job import LeotestTask, LeotestJobCron, LeotestJobAtq, LeotestRun,\
    check_schedule_conflict_list, check_schedule_conflict_range, find_empty_slot_till_job_end,\
    get_event_list_from_job_list
from common.user import LeotestUser, LeotestUserRoles
from common.node import LeotestNode
from common.trigger import verify_trigger_default

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")

log = logging.getLogger(__name__)

class CheckToken(object):
    """Decorator that parses the gRPC headers to fetch the access token (either 
    the static access token or the JWT token). Thereafter, it verifies whether 
    the access token is valid or not. If it is valid, it sets the ``userid`` and 
    the ``role`` (after fetching it from the datastore) in the gRPC context. If the
    access token verification fails, it raises an ``UNAUTHENTICATED`` status code.
    """
    def __init__(self, empty_response_cls, grpc_status_code=grpc.StatusCode.UNAUTHENTICATED, grpc_details=""):
        self._empty_rsp_cls = empty_response_cls
        self._status = grpc_status_code
        self._details = grpc_details

    def __call__(self, f):
        @wraps(f)
        def wrapped_function(slf, request, context):
            meta = context.invocation_metadata()
            creds = {'userid': None, 'access_token': None, 'jwt_access_token': None}
            for item in meta:
                if item[0] == "x-leotest-access-token":
                    creds['access_token'] = item[1]
                
                if item[0] == "x-leotest-userid":
                    creds['userid'] = item[1]

                if item[0] == "x-leotest-jwt-access-token":
                    creds['jwt_access_token'] = item[1]   

            log.info('[CheckToken] %s' % str(creds))

            # if JWT token is present 
            if creds['jwt_access_token']:
                ret = slf.verify_jwt(creds['jwt_access_token'])
                if ret:
                    userid, role = ret 
                    log.info('[CheckToken] setting context: userid=%s role=%s' 
                            % (userid, role))
                    context.creds_userid = userid
                    context.creds_role = role
                    return f(slf, request, context)

            # otherwise, if userid and access_token is present 
            if creds['userid'] and creds['access_token']:
                # use this if you want to update the context and pass some data to the method handler
                # context.user_by_token = "sample_user" 

                # verify role and set context 
                role = slf.verify_user(creds['userid'], 
                                        creds['access_token'])
                if role is not None:
                    log.info('[CheckToken] setting context: userid=%s role=%s' 
                            % (creds['userid'], role))
                    context.creds_userid = creds['userid']
                    context.creds_role = role
                    return f(slf, request, context)
            
            context.set_code(self._status)
            context.set_details(self._details)
            return self._empty_rsp_cls()
        return wrapped_function

class LeotestOrchestratorGrpc(pb2_grpc.LeotestOrchestrator):
    """Implementation of the gRPC service exposed by the orchestrator. The class
    implements all the rpc's defined in the `common/leotest.proto` protocol 
    definition. Incoming rpc requests are verified by :class:`CheckToken` 
    before being invoked. 

    :param db_server: Hostname of the MongoDB database server, defaults to 'localhost'
    :type db_server: str, optional
    :param db_port: Port number of the MongoDB database server, defaults to 27017
    :type db_port: int, optional
    :param db_name: Name of the database, defaults to 'leotest'
    :type db_name: str, optional
    :param admin_access_token: Static access token to use to initialize the 
        `admin` username, defaults to 'leotest-access-token'
    :type admin_access_token: str, optional
    :param jwt_secret: JWT secret to use for authenticating the gRPC requests, defaults to ''
    :type jwt_secret: str, optional
    :param jwt_algo: JWT algorithm to use for authenticating the gRPC requests, defaults to ''
    :type jwt_algo: str, optional
    """
    def __init__(self, db_server='localhost', db_port=27017, 
                 db_name='leotest', admin_access_token='leotest-access-token',
                 jwt_secret='', jwt_algo=''):
        """Constructor for the orchestartor's gRPC service.

        :param db_server: hostname of the MongoDB database server, defaults to 'localhost'
        :type db_server: str, optional
        :param db_port: port number of the MongoDB database server, defaults to 27017
        :type db_port: int, optional
        :param db_name: name of the database, defaults to 'leotest'
        :type db_name: str, optional
        :param admin_access_token: static access token to use to initialize the 
            `admin` username, defaults to 'leotest-access-token'
        :type admin_access_token: str, optional
        :param jwt_secret: JWT secret to use for authenticating the gRPC requests, defaults to ''
        :type jwt_secret: str, optional
        :param jwt_algo: JWT algorithm to use for authenticating the gRPC requests, defaults to ''
        :type jwt_algo: str, optional
        """
        self.db = LeotestDatastoreMongo(
            server=db_server, port=db_port, database=db_name, 
            admin_access_token=admin_access_token)
        
        self.jwt_secret = jwt_secret
        self.jwt_algo = jwt_algo

    @CheckToken(pb2.message_heartbeat_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def report_heartbeat(self, request, context):
        """RPC routine to register a heartbeat with the orchestrator. 

        :param request: gRPC request message for ``report_heartbeat``
        :type request: :class:`pb2.message_heartbeat`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: ``received`` is set to ``True`` if successful else ``False``
        :rtype: :class:`pb2.message_heartbeat_response`
        """        
        nodeid = request.nodeid
        _userid = context.creds_userid
        _role = context.creds_role
        _role_name = LeotestUserRoles(_role).name

        log.info("[heartbeat] userid=%s nodeid=%s" % (_userid, nodeid))

        if nodeid == _userid:
            log.info("[heartbeat] marked nodeid=%s" % (nodeid))
            self.db.mark_node(nodeid)
            result = {'received': True}
        else:
            log.info("[heartbeat] nodeid (%s) and userid (%s) do not match, not marking." 
                            % (nodeid, _userid))
            result = {'received': False}
        return pb2.message_heartbeat_response(**result)

    @CheckToken(pb2.message_update_global_config_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def update_global_config(self, request, context):
        """Update global configuration for the testbed. Only users with an 
        ``ADMIN`` role can invoke the RPC. For users having roles other than 
        ``ADMIN``, the config is not updated and the ``state`` is set to ``1``
        in the response message. 

        :param request: gRPC request message for ``update_global_config``
        :type request: :class:`pb2.message_update_global_config`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: Response indicating response state and a detailed message. 
            ``state==0`` implies that the request is successful. ``state==1`` implies 
            that there was some error encountered while processing the request. 
            ``message`` field contains a detailed message in all cases.
        :rtype: :class:`pb2.message_update_global_config_response`
        """
        log.info("[update_global_config]")

        _userid = context.creds_userid
        _role = context.creds_role
        _role_name = LeotestUserRoles(_role).name

        if _role == LeotestUserRoles.ADMIN.value:
            config = MessageToDict(request.config) 
            log.info(config)
            state, msg = self.db.update_config(config)
        else:
            state = 1
            msg = "permission denied: userid=%s and role=%s" % (_userid, _role_name)

        result = {
            'state': state,
            'message': msg
        }
        return pb2.message_update_global_config_response(**result)

    @CheckToken(pb2.message_get_global_config_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def get_global_config(self, request, context):
        """Get global configuration for the testbed.

        :param request: gRPC request message for ``get_global_config``
        :type request: :class:`pb2.message_get_global_config`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: Returns the global config of the testbed.
        :rtype: :class:`pb2.message_get_global_config_response`

        .. todo::
            Allow only users with ``ADMIN`` or ``NODE`` role to access the rpc
        """        
        log.info("[get_global_config]")
        
        state, config = self.db.get_config()
        config.pop('_id')
        config.pop('id')
        result = {
            'config': config
        }

        log.info(result)
        return pb2.message_get_global_config_response(**result)

    @CheckToken(pb2.message_register_user_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def register_user(self, request, context):
        """Register a user on LEOScope. Only users with ``ADMIN`` role can 
        register a user. 

        :param request: gRPC request message for ``register_user``
        :type request: :class:`pb2.message_register_user`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: Returns a response message consisting of ``state`` and ``message``.
            ``state`` is set to ``0`` if the request has succeeded, else ``1``. 
            ``message`` contains the detailed message in all cases.
        :rtype: :class:`pb2.message_register_user_response`
        """
        # pb2.leopard_node_state.Name(request.state)
        id = request.id
        name = request.name
        role = request.role
        team = request.team

        role_name = pb2.user_roles.Name(role)

        _userid = context.creds_userid
        _role = context.creds_role
        _role_name = LeotestUserRoles(_role).name

        log.info("[register_user] id=%s name=%s role=%s team=%s" 
                                % (id, name, role_name, team))

        if _role == LeotestUserRoles.ADMIN.value:
            hl = hashlib.sha256()
            hl.update(id.encode('utf-8'))
            hl.update(name.encode('utf-8'))
            hl.update(team.encode('utf-8'))
            access_token = hl.hexdigest()
            state, msg = self.db.add_user(
                LeotestUser(id, name, role, team, static_access_token=access_token))

            if state == 0:
                msg += " access_token=%s" % access_token
        else:
            state = 1
            msg = "permission denied: userid=%s and role=%s" % (_userid, _role_name)

        log.info("[register_user_response] id=%s state=%s message=%s" % (id, state, msg))
        result = {
            'state': state,
            'message': msg
        }
        
        return pb2.message_register_user_response(**result)
    
    @CheckToken(pb2.message_get_user_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def get_user(self, request, context):
        """Get a user with a given ``userid``.

        :param request: gRPC request message for ``get_user``
        :type request: :class:`pb2.message_get_user`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the user with the given ``userid`` exists, ``exists`` is set 
            to ``True`` and the message contains user information such as 
            ``userid``, ``name``, ``role``, ``team``.
        :rtype: :class:`pb2.message_get_user_response`
        """
        userid = request.userid
        log.info("[get_user] id=%s" % (userid))
        user = self.db.get_user(userid) 
        if user:
            result = {
                'exists': True,
                'id': user.id,
                'name': user.name,
                'role': user.role,
                'team': user.team
            }
        else:
            result = {
                'exists': False,
                'id': '',
                'name': '',
                'role': 'USER',
                'team': ''
            }

        log.info("[get_user_response] id=%s exists=%s name=%s role=%s team=%s" 
                                    % (userid, 
                                    str(result['exists']), 
                                    result['name'],
                                    result['role'],
                                    result['team']))
        return pb2.message_get_user_response(**result)

    @CheckToken(pb2.message_modify_user_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def modify_user(self, request, context):
        """Modify a user with a given ``userid``.

        :param request: gRPC request message for ``modify_user``
        :type request: :class:`pb2.message_modify_user`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the user with the given ``userid`` exists, ``state`` is set 
            to ``1`` else it is set to ``0``. In both cases ``message`` contains
            detailed information about the result. 
        :rtype: :class:`pb2.message_modify_user_response`
        """
        id = request.id
        name = request.name
        role = request.role
        team = request.team
        role_name = pb2.user_roles.Name(role)

        _userid = context.creds_userid
        _role = context.creds_role
        _role_name = LeotestUserRoles(_role).name
        
        log.info("[modify_user] id=%s with params name=%s role=%s team=%s" 
                                    % (id, name, role_name, team))
        if _role == LeotestUserRoles.ADMIN.value:
            state, msg = self.db.modify_user(LeotestUser(id, name, role, team)) 
        
        else:
            state = 1
            message = "permission denied: userid=%s and role=%s" % (_userid, _role_name)

        result = {
            'state': state,
            'message': msg
        }
        return pb2.message_modify_user_response(**result)

    @CheckToken(pb2.message_delete_user_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def delete_user(self, request, context):
        
        _userid = context.creds_userid
        _role = context.creds_role
        _role_name = LeotestUserRoles(_role).name

        if _role == LeotestUserRoles.ADMIN.value:
            userid = request.userid
            log.info("[delete_user] id=%s " % (userid))
            state, msg = self.db.delete_user(LeotestUser(userid, '', '', '')) 
        
        else:
            state = 1
            msg = "permission denied: userid=%s and role=%s" % (_userid, _role_name)

        result = {
            'state': state,
            'message': msg
        }
        return pb2.message_delete_user_response(**result)
    
    def verify_user(self, userid, access_token):
        """Given ``userid`` and ``access_token`` verify and return the user role.

        :param userid: Userid 
        :type userid: str
        :param access_token: The access token, either static access token or 
            JWT access token
        :type access_token: str
        :return: If the access token is valid, return the user role, else None
        :rtype: int or None
        """

        log.info("[verify_user] id=%s access_token=%s" % (userid, access_token))
        user = self.db.get_user(userid) 

        if user:
            _static_access_token = user.static_access_token
            _access_token = user.access_token
            
            log.info("[verify_user] userid=%s found and has following tokens: " 
                     "static=%s dynamic=%s" 
                     % (userid, _static_access_token, _access_token))

            if _static_access_token == access_token or _access_token == access_token:
                log.info("[verify_user] user validated: userid=%s access_token=%s role=%s" 
                                        % (userid, access_token, user.role))
                return user.role 
            else:
                log.info("[verify_user] userid=%s invalid access token" % userid)
                return None 
        else:
            log.info("[verify_user] userid=%s not found" % userid)
            return None 
    
    def verify_jwt(self, jwt_access_token):
        """Given JWT access token, verify the token and get ``userid``, ``role``

        :param jwt_access_token: JWT access token
        :type jwt_access_token: str
        :return: Returns userid and user role
        :rtype: (str, :class:`LeotestUserRoles`)
        """        

        log.info("[verify_jwt] jwt_access_token=%s" % (jwt_access_token))
        try:
            payload = jwt.decode(jwt_access_token, 
                                self.jwt_secret, algorithms=[self.jwt_algo])
            
            userid = payload.get("userid")
            user = self.db.get_user(userid)
            if user:
                log.info("[verify_jwt] user validated: userid=%s role=%s" 
                                        % (userid, user.role))
                return userid, user.role 
            else:
                log.info("[verify_jwt] userid=%s not found" % userid)
                return None

        except JWTError as e:
            log.info("[verify_jwt] exception=%s" % str(e))
            return None 

    @CheckToken(pb2.message_schedule_job_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def schedule_job(self, request, context):
        """Schedule on a job LEOScope. The RPC call has the following flow:
            - Verify if there a conflict with the existing jobs on the ``nodeid``.
            - If ``server`` is specified, verify if there is a conflict with 
                existing jobs on ``server``.
            - If trigger is specified, verify the trigger syntax.
            - If all is good, write the job schedule to the database.

        :param request: gRPC request message for ``schedule_job``
        :type request: :class:`pb2.message_schedule_job`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the job is scheduled successfully, ``state`` is set 
            to ``0`` else it is set to ``1``. In both cases ``message`` contains
            detailed information about the result.  
        :rtype: :class:`pb2.message_schedule_job_response`
        """

        userid = context.creds_userid
        role = context.creds_role

        state = 1
        msg = "unknown error"
        schedule_conflict = False

        id = request.id
        nodeid = request.nodeid 
        _type = request.type 
        type_name = pb2.job_type.Name(_type)
        params = MessageToDict(request.params)
        schedule = request.schedule
        start_date = request.start_date 
        end_date = request.end_date 
        length_secs = request.length_secs
        overhead = request.overhead
        server = request.server if request.HasField('server') else None
        trigger = request.trigger if request.HasField('trigger') else None
        config = request.config

        log.info("[schedule_job] userid=%s jobid=%s nodeid=%s type=%s params" 
            "\{mode=%s deploy=%s execute=%s finish=%s\} schedule=%s"
            " start_date=%s end_date=%s length_secs=%d overhead=%s server=%s trigger=%s"
            % (userid, id, nodeid, type_name, params['mode'], params['deploy'], 
                params['execute'], params['finish'], schedule, 
                start_date, end_date, length_secs, overhead, server, trigger))

        # print(id, nodeid, type_name, params, schedule) 
        if type_name.lower() == "cron" or type_name.lower() == "atq":
            if overhead==True:
                jobs = []
                exists1 = None 
                exists2 = None
                # check schedule conflict with existing jobs on the node and server
                exists1, jobs_nodeid = self.db.get_jobs_by_nodeid(nodeid=nodeid)
                if exists1:
                    jobs.extend(jobs_nodeid)
                
                if server:
                    exists2, jobs_server = self.db.get_jobs_by_nodeid(nodeid=server)
                    if exists2:
                        jobs.extend(jobs_server)
                        
                if exists1 or exists2:
                    log.info("[schedule_job][jobid=%s][nodeid=%s][type=%s]" 
                            " existing jobs found. Checking conflicts..." 
                            % (id, nodeid, type_name))

                    print('jobs', jobs)
                    job_list = []
                    for job in jobs:
                        if job.overhead==True:
                            if job.type.lower() == 'cron':
                                cron = job.get_cron_string()
                            else:
                                cron = ''
                            res = {
                                'id': job.jobid,
                                'nodeid': job.nodeid,
                                'type': job.type.lower(),
                                'params': job.job_params,
                                'cron': cron,
                                'start': job.start_date,
                                'end': job.end_date, 
                                'length': job.length_secs
                            }
                            job_list.append(res)

                    curr_job = {
                        'type': type_name.lower(),
                        'cron': schedule,
                        'start': start_date,
                        'end': end_date,
                        'length': length_secs
                    }

                    conflicts = check_schedule_conflict_list(curr_job, job_list)

                    if len(conflicts) > 0:
                        log.info("[schedule_job][jobid=%s][nodeid=%s][type=%s]" 
                            " conflicts with existing jobs found. Aborting." 
                            % (id, nodeid, type_name))
                        msg = "schedule conflict:"
                        for i, conflict in enumerate(conflicts):
                            sched = conflict['sched']
                            conflict_range = conflict['range']
                            msg += "<%d, jobid=%s, cron='%s', start=%s, end=%s,"\
                                    "conflict_start=%s, conflict_end=%s>"\
                                    % (i, sched['id'], sched['cron'], 
                                        sched['start'], sched['end'], 
                                        conflict_range['start'], conflict_range['end'])
                        
                        state = 1
                        schedule_conflict = True

            
            if not schedule_conflict:
                log.info("[schedule_job][jobid=%s][nodeid=%s][type=%s]" 
                        " no conflicts with existing jobs found. Attempting to schedule." 
                        % (id, nodeid, type_name))
                
                valid = True 
                if trigger != None:
                    # verify sanity of trigger 
                    valid, err_msg = verify_trigger_default(trigger)
                    if not valid:
                        # send error message 
                        state = 1 
                        msg = err_msg
                
                if valid:
                    if type_name.lower() == 'cron':
                        job = LeotestJobCron(jobid=id, 
                                            nodeid=nodeid, 
                                            userid=userid,
                                            job_params=params,
                                            start_date=start_date, 
                                            end_date=end_date, 
                                            length_secs=length_secs,
                                            overhead=overhead,
                                            server=server,
                                            trigger=trigger,
                                            config=config)
                        job.set_schedule_cron(schedule)
                    
                    else:
                        job = LeotestJobAtq(jobid=id, 
                                            nodeid=nodeid, 
                                            userid=userid,
                                            job_params=params,
                                            start_date=start_date, 
                                            end_date=end_date, 
                                            length_secs=length_secs,
                                            overhead=overhead,
                                            server=server,
                                            trigger=trigger,
                                            config=config)
                    print("Orchestrator : Job added %s" %(id)) 
                    state, msg = self.db.add_job(job)

        result = {
            'state': state,
            'message': msg
        }
        return pb2.message_schedule_job_response(**result)

    @CheckToken(pb2.message_reschedule_job_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def reschedule_job_nearest(self, request, context):
        """Reschedule a job to the nearest empty slot. The rpc receives 
        a ``jobid`` and a ``start_time`` and an ``end_time`` and searchers for 
        the nearest empty slot that is large enough for the job to "fit", that is,
        the time length of the free slot is greater than or equal to the job
        length. 

        :param request: gRPC request message for ``reschedule_job_nearest``
        :type request: :class:`pb2.message_reschedule_job`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the job has been rescheduled, ``rescheduled`` is set 
            to ``0`` else it is set to ``1``. In both cases ``message`` contains
            detailed information about the result. 
        :rtype: :class:`pb2.message_reschedule_job_response`
        """
        state = False
        msg = "unknown error"
        schedule_conflict = False
        jobid = request.jobid 
        start_time = datetimeParse(request.starttime)
        end_time = datetimeParse(request.endtime)
        log.info("[reschedule_job_nearest] jobid=%s" % (jobid))
        exists, job = self.db.get_job_by_id(jobid)

        if exists:
            nodeid = job.nodeid
            job_type = job.type 
            length = int(job.length_secs)
            log.info("[reschedule_job_nearest] jobid=%s type=%s nodeid=%s" 
                                                % (jobid, job_type, nodeid))
            if job_type == 'atq':
                log.info('working with job type atq')
                exists, node_jobs = self.db.get_jobs_by_nodeid(nodeid=nodeid)
                if exists:
                    log.info('[reschedule_job_nearest] jobs exist, finding nearest empty slot')
                    job_list = []
                    for node_job in node_jobs:
                        if node_job.jobid == job.jobid:
                            continue 
                        if node_job.type == 'cron':
                            cron = node_job.get_cron_string()
                        else:
                            cron = ''

                        job_list.append({
                            'start': node_job.start_date,
                            'end': node_job.end_date,
                            'cron': cron,
                            'length': node_job.length_secs,
                            'type': node_job.type
                        })
                    # check for nearest time slot 
                    next_slot = find_empty_slot_till_job_end(
                        start_time, end_time, length, 
                        job_list)

                    if next_slot:
                        log.info('[reschedule_job_nearest] found next slot: %s' 
                                                                % str(next_slot))
                        end_time = job.end_date
                        self.db.update_job_date(jobid, next_slot, end_time)
                        state = True
                        msg = "updated job schedule successfully; next_slot=%s" % str(next_slot)
                    else:
                        log.info('[reschedule_job_nearest] empty slot not found ' 
                                 'with the given constraints: start_time=%s end_time=%s' 
                                                        % (str(start_time), str(end_time)))

                        state = False 
                        msg = 'empty slot not found with the given constraints:'\
                            'start_time=%s end_time=%s' % (str(start_time), str(end_time))
                
                else:
                    log.info('[reschedule_job_nearest] no jobs exist; rescheduling after 10 minutes')
                    next_slot = start_time + timedelta(minutes=10)
                    log.info('[reschedule_job_nearest] no jobs exist; rescheduling after 10 minutes;'
                             'next_slot=%s' % (str(next_slot)))
                    self.db.update_job_date(jobid, next_slot, end_time)
                    state = True
                    msg = "updated job schedule successfully; next_slot=%s" % str(next_slot)
            else:
                state = False
                msg = "only jobs of type 'atq' can be rescheduled."
        else:
            state = False
            msg = "job with the given id not found."

        result = {
            'rescheduled': state,
            'message': msg
        }

        log.info(result)
        return pb2.message_reschedule_job_response(**result)

    @CheckToken(pb2.message_get_job_by_id_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def get_job_by_id(self, request, context):
        """Get job with a given ``jobid``.

        :param request: gRPC request message for ``modify_user``
        :type request: :class:`pb2.message_modify_user`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the user with the given ``userid`` exists, ``state`` is set 
            to ``1`` else it is set to ``0``. In both cases ``message`` contains
            detailed information about the result. 
        :rtype: :class:`pb2.message_modify_user_response`
        """

        jobid = request.jobid
        log.info("[get_job_by_id] jobid=%s" % (jobid))
        exists, job = self.db.get_job_by_id(jobid)
        _type = pb2.job_type.Value(job.type.upper())
        schedule = job.get_cron_string() if job.type.upper() == "CRON" else "" 

        if exists:
            result = {
                'exists': True,
                'id': job.jobid,
                'nodeid': job.nodeid,
                'userid': job.userid,
                'start_date': job.start_date, 
                'end_date': job.end_date,
                'type': _type,
                'params': job.job_params,
                'schedule': schedule,
                'trigger': job.trigger,
                'server': job.server,
                'overhead': job.overhead,
                'config': job.config
            }
        else:
            result = {
                'exists': False,
            }    
        
        return pb2.message_get_job_by_id_response(**result)
    

    @CheckToken(pb2.message_get_jobs_by_userid_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def get_jobs_by_userid(self, request, context):
        """Get jobs scheduled by a given user.

        :param request: gRPC request message for ``get_jobs_by_userid``
        :type request: :class:`pb2.message_get_jobs_by_userid`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the user with the given ``userid`` exists, ``exists`` is set 
            to ``True`` and ``jobs`` contains the list of all jobs scheduled 
            by the user. If the user does not exist, ``exists`` is set to 
            ``False``. In both cases ``message`` contains detailed information 
            about the result. 
        :rtype: :class:`pb2.message_get_jobs_by_userid_response`
        """

        _userid = context.creds_userid
        _role = context.creds_role
        role_name = LeotestUserRoles(_role).name

        if _role == LeotestUserRoles.ADMIN.value or _userid == userid:

            userid = request.userid
            log.info("[get_jobs_by_userid] userid=%s" % (userid))

            exists, jobs = self.db.get_jobs_by_userid(userid=userid)
            if exists:
                job_list = []
                for job in jobs:
                    _type = pb2.job_type.Value(job.type.upper())

                    if job.type.lower() == 'cron':
                        res = {
                            'id': job.jobid,
                            'nodeid': job.nodeid,
                            'userid': job.userid,
                            'type': _type,
                            'params': job.job_params,
                            'schedule': job.get_cron_string(),
                            'start_date': job.start_date, 
                            'end_date': job.end_date,
                            'length_secs': job.length_secs,
                            'overhead': job.overhead,
                            'trigger': job.trigger,
                            'server': job.server
                        }

                    elif job.type.lower() == 'atq':
                        res = {
                            'id': job.jobid,
                            'nodeid': job.nodeid,
                            'userid': job.userid,
                            'type': _type,
                            'params': job.job_params,
                            'schedule': '',
                            'start_date': job.start_date, 
                            'end_date': job.end_date,
                            'length_secs': job.length_secs,
                            'overhead': job.overhead,
                            'trigger': job.trigger,
                            'server': job.server
                        }

                    job_list.append(res)
                
                result = {
                    'exists': True,
                    'jobs': job_list
                }
            
            else:
                result = {
                    'exists': False
                }
        
        else:
            result = {
                'exists': False, 
                'message': "permission denied: userid=%s and role=%s" % (_userid, role_name)
            }
        
        return pb2.message_get_jobs_by_userid_response(**result)

    @CheckToken(pb2.message_get_jobs_by_nodeid_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def get_jobs_by_nodeid(self, request, context):
        """Get jobs scheduled on a given node.

        :param request: gRPC request message for ``get_jobs_by_nodeid``
        :type request: :class:`pb2.message_get_jobs_by_nodeid`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the node with the given ``nodeid`` exists, ``exists`` is set 
            to ``True`` and ``jobs`` contains the list of all jobs scheduled 
            on the node. If the node does not exist, ``exists`` is set to 
            ``False``. 
        :rtype: :class:`pb2.message_get_jobs_by_nodeid_response`
        """
                
        nodeid = request.nodeid
        log.info("[get_jobs_by_nodeid] nodeid=%s" % (nodeid))

        exists, jobs = self.db.get_jobs_by_nodeid(nodeid=nodeid)
        if exists:
            job_list = []
            for job in jobs:
                _type = pb2.job_type.Value(job.type.upper())

                if job.type.lower() == 'cron':
                    res = {
                        'id': job.jobid,
                        'nodeid': job.nodeid,
                        'userid': job.userid,
                        'type': _type,
                        'params': job.job_params,
                        'schedule': job.get_cron_string(),
                        'start_date': job.start_date, 
                        'end_date': job.end_date,
                        'length_secs': job.length_secs,
                        'overhead': job.overhead,
                        'trigger': job.trigger,
                        'server': job.server
                    }

                elif job.type.lower() == 'atq':
                    res = {
                        'id': job.jobid,
                        'nodeid': job.nodeid,
                        'userid': job.userid,
                        'type': _type,
                        'params': job.job_params,
                        'schedule': '',
                        'start_date': job.start_date, 
                        'end_date': job.end_date,
                        'length_secs': job.length_secs,
                        'overhead': job.overhead,
                        'trigger': job.trigger,
                        'server': job.server
                    }

                job_list.append(res)
            
            result = {
                'exists': True,
                'jobs': job_list
            }
        
        else:
            result = {
                'exists': False
            }
        
        return pb2.message_get_jobs_by_nodeid_response(**result)
    
    def modify_job(self, request, context):
        """Modify an existing job.

        :param request: gRPC request message for ``modify_job``
        :type request: :class:`pb2.message_get_jobs_by_nodeid`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the node with the given ``nodeid`` exists, ``exists`` is set 
            to ``True`` and ``jobs`` contains the list of all jobs scheduled 
            on the node. If the node does not exist, ``exists`` is set to 
            ``False``. 
        :rtype: :class:`pb2.message_get_jobs_by_nodeid_response`

        .. todo::
            Implement the rpc. Only allow users with role ``ADMIN`` and the owners
            of the job to modify a job. 
        """

    @CheckToken(pb2.message_delete_job_by_id_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def delete_job_by_id(self, request, context):
        """Delete a given job using jobid. Only users with role ``ADMIN`` and the
        user who owns the job can delete a job. 

        :param request: gRPC request message for ``delete_job_by_id``
        :type request: :class:`pb2.message_delete_job_by_id`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the job with the given ``jobid`` exists, the job is deleted
            and ``exists`` is set to ``True``. If the job does not exist, 
            ``exists`` is set to ``False``. In all cases, ``message`` contains 
            the detailed message about the result of the request.  
        :rtype: :class:`pb2.message_delete_job_by_id_response`
        """

        jobid = request.jobid
        _userid = context.creds_userid
        _role = context.creds_role
        role_name = LeotestUserRoles(_role).name
        log.info("[delete_job_by_id] jobid=%s" % (jobid))
        # access control: only admin or owner of the job can delete the job 

        allowed = False 
        exists = True 
        if _role == LeotestUserRoles.ADMIN.value:
            allowed = True 
        else:
            # get owner of job from db 
            exists, job = self.db.get_job_by_id(jobid)
            
            if exists:
                log.info("db_user_id=%s _userid=%s" % (job.userid, _userid))
                if job.userid == _userid:
                    allowed = True 
            else:
                log.info("[delete_job_by_id] job with jobid=%s not found" % (jobid))
                allowed = True 
        
        if  allowed and exists:
            exists, msg = self.db.delete_job_by_id(jobid)
        else:
            if not allowed: 
                msg = "permission denied: userid=%s and role=%s" % (_userid, role_name)
            else:
                msg = "job with jobid=%s does not exist" % jobid

        result = {
            'exists':  exists,
            'message': msg
        }

        return pb2.message_delete_job_by_id_response(**result)
    
    @CheckToken(pb2.message_delete_jobs_by_nodeid_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def delete_jobs_by_nodeid(self, request, context):
        """Delete all jobs on a given nodeid. Only users with role ``ADMIN`` can
        delete all jobs on a node.

        :param request: gRPC request message for ``delete_jobs_by_nodeid``
        :type request: :class:`pb2.delete_jobs_by_nodeid`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: ``num_deleted`` contains the number of jobs deleted. ``message``
            contains a message with detailed information about the status of the 
            request.  
        :rtype: :class:`pb2.message_delete_job_by_id_response`
        """
        nodeid = request.nodeid
        log.info("[delete_jobs_by_nodeid] nodeid=%s" % (nodeid))
        count, msg = self.db.delete_jobs_by_nodeid(nodeid)
        result = {
            'num_deleted': count,
            'message': msg
        }
        return pb2.message_delete_jobs_by_nodeid_response(**result)

    @CheckToken(pb2.message_update_run_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def update_run(self, request, context):
        """Update run status. 

        :param request: gRPC request message for ``update_run``
        :type request: :class:`pb2.update_run`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If update is successful ``state`` is set to ``0``, otherwise 
            ``1``. In both cases, ``message`` conatins the detailed information
            about the status of the request.  
        :rtype: :class:`pb2.message_delete_job_by_id_response`

        .. todo::
            Allow only users with role ``NODE`` to be able to update run status. 
        """
        runid = request.run.runid 
        jobid = request.run.jobid 
        nodeid = request.run.nodeid 
        userid = request.run.userid
        start_time = request.run.start_time 
        end_time = request.run.end_time 
        last_updated = request.run.last_updated
        blob_url = request.run.blob_url 
        status = request.run.status 
        status_message = request.run.status_message

        log.info('[update_run] jobid=%s runid=%s' % (jobid, runid))

        run = LeotestRun(
            runid=runid, 
            jobid=jobid, 
            nodeid=nodeid, 
            userid=userid,
            start_time=start_time, 
            end_time=end_time,
            last_updated=last_updated,
            blob_url=blob_url,
            status=status, 
            status_message=status_message)
        state, message = self.db.update_run(run)
        self.db.mark_node(nodeid)
        result = {
            'state': state,
            'message': message
        }

        log.info('[update_run] sending reply jobid=%s runid=%s state=%s message=%s' 
                                % (jobid, runid, str(state), message))
        return pb2.message_update_run_response(**result)

    @CheckToken(pb2.message_get_runs_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def get_runs(self, request, context):
        """Get a list of runs after filtering them on certain parameters. The 
        list of paramaters currently accepted are: ``runid``, ``jobid``, ``nodeid``, 
        ``userid``, ``time_range``, ``limit``.

        All the parameters are optional. Client can specify one or more of the 
        parameters. If more than one parameters are provided, then a combination
        of them is used to filter the runs. 
            
        :param request: gRPC request message for ``get_runs``
        :type request: :class:`pb2.message_get_runs`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``runs`` contains the list of 
            all runs.
        :rtype: :class:`pb2.message_get_runs_response`
        
        .. todo::
            Allow only users with role ``NODE`` to be able to update run status. 
        """

        runid = None
        if request.HasField('runid'):
            runid = request.runid

        jobid = None
        if request.HasField('jobid'):
            jobid = request.jobid
        
        nodeid = None
        if request.HasField('nodeid'):
            nodeid = request.nodeid
    
        userid = None
        if request.HasField('userid'):
            userid = request.userid
        
        time_range = None
        if request.HasField('time_range'):
            time_range = request.time_range
        
        limit = None
        if request.HasField('limit'):
            limit = request.limit
        
        runs = []
        res = self.db.get_runs(runid=runid,
                                jobid=jobid, 
                                nodeid=nodeid, 
                                userid=userid,
                                time_range=time_range, 
                                limit=limit)
        for run in res:
            runs.append({
                'runid': run.runid, 
                'jobid': run.jobid,
                'nodeid': run.nodeid, 
                'userid': run.userid,
                'last_updated': str(run.last_updated),
                'start_time': str(run.start_time), 
                'end_time': str(run.end_time),
                'blob_url': run.blob_url,
                'status': run.status,
                'status_message': run.status_message
            })
        
        result = {
            'runs': runs
        }
        log.info('runs %s' % str(runs))
        return pb2.message_get_runs_response(**result)
    
    @CheckToken(pb2.message_register_node_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")
    def register_node(self, request, context):
        """Register a node on LEOScope. Only a user with role ``ADMIN`` can 
        register a node.
            
        :param request: gRPC request message for ``register_node``
        :type request: :class:`pb2.message_register_node`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``state`` si set to ``0``, otherwise 
            it is set to ``1``. In both cases, ``message`` contains details 
            about the status of the request.
        :rtype: :class:`pb2.message_register_node_response`
        """
        
        userid = context.creds_userid
        role = context.creds_role
        
        log.info('[register_node] nodeid=%s' % request.node.nodeid)
        if role==LeotestUserRoles.ADMIN.value:
            
            nodeid = request.node.nodeid
            name = request.node.name
            team = request.node.description

            hl = hashlib.sha256()
            hl.update(nodeid.encode('utf-8'))
            hl.update(name.encode('utf-8'))
            hl.update(team.encode('utf-8'))
            access_token = hl.hexdigest()
            # add the node to the users table 
            node_user = LeotestUser(id=nodeid, name=name, 
                    role=LeotestUserRoles.NODE.value, team=team,
                    static_access_token = access_token)

            ret = self.db.add_user(node_user)

            state, message = self.db.register_node(
                                LeotestNode(**MessageToDict(request)['node']))
            
            if state == 0:
                message += " access_token=%s" % access_token
        else:
            state = 1
            role_name = LeotestUserRoles(role).name
            message = "permission denied: userid=%s and role=%s" % (userid, role_name)

        result = {
            'state': state,
            'message': message
        }
        return pb2.message_register_node_response(**result)

    @CheckToken(pb2.message_delete_node_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")    
    def delete_node(self, request, context):
        """Delete a node with a given ``nodeid``.
            
        :param request: gRPC request message for ``delete_node``
        :type request: :class:`pb2.message_delete_node`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``state`` is set to ``0``, otherwise 
            it is set to ``1``. In both cases, ``message`` contains details 
            about the status of the request.
        :rtype: :class:`pb2.message_delete_node_response`
        """

        nodeid = request.nodeid
        delete_jobs = request.deleteJobs

        userid = context.creds_userid
        role = context.creds_role

        if role==LeotestUserRoles.ADMIN.value:
            state, message = self.db.delete_node(nodeid=nodeid, 
                                                delete_jobs=delete_jobs)
            
            self.db.delete_user(LeotestUser(nodeid, '', '', '')) 
        else:
            state = 1
            role_name = LeotestUserRoles(role).name
            message = "permission denied: userid=%s and role=%s" % (userid, role_name)

        result = {
            'state': state,
            'message': message
        }
        return pb2.message_delete_node_response(**result)

    @CheckToken(pb2.message_get_nodes_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def get_nodes(self, request, context):
        """Get a list of nodes after filtering them on certain parameters.
        All parameters are optional. When multiple parameters are specified, 
        a combination of them is used to filter the nodes. The supported 
        parameters are: ``nodeid``, ``location``, ``name``, ``provider``,
        ``active``, ``activeThres``.
            
        :param request: gRPC request message for ``get_nodes``
        :type request: :class:`pb2.message_get_nodes`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: ``nodes`` contains a list of all the filtered nodes.
        :rtype: :class:`pb2.message_get_nodes_response`
        """

        nodeid = request.nodeid if request.HasField('nodeid') else None
        location = request.location if request.HasField('location') else None
        name = request.name if request.HasField('name') else None
        provider = request.provider if request.HasField('provider') else None
        active = request.active if request.HasField('active') else False
        activeThres = request.activeThres if request.HasField('activeThres') else 600

        log.info('[get_nodes] nodeid=%s location=%s name=%s provider=%s active=%s'
                ' activeThres=%s' % (nodeid, location, name, provider, active, activeThres))

        nodes = self.db.get_nodes(nodeid, location, name, provider, active, activeThres)
        log.info('nodes=%s' % str(nodes))
        result = {'nodes': nodes}
        return pb2.message_get_nodes_response(**result)
    
    @CheckToken(pb2.message_update_node_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def update_node(self, request, context):
        """Get a list of nodes after filtering them on certain parameters.
        All parameters are optional. When multiple parameters are specified, 
        a combination of them is used to filter the nodes. The supported 
        parameters are: ``nodeid``, ``location``, ``name``, ``provider``,
        ``active``, ``activeThres``.
            
        :param request: gRPC request message for ``get_nodes``
        :type request: :class:`pb2.message_update_node`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: ``nodes`` contains a list of all the filtered nodes.
        :rtype: :class:`pb2.message_update_node_response`
        """

        nodeid = request.nodeid 
        name = request.name if request.HasField('name') else None
        description = request.description if request.HasField('description') else None
        last_active = request.last_active if request.HasField('last_active') else None
        coords = request.last_active if request.HasField('coords') else None
        location = request.location if request.HasField('location') else None
        provider = request.provider if request.HasField('provider') else None
        public_ip = request.public_ip if request.HasField('public_ip') else None

        log.info('[update_node] nodeid=%s' 
                   'name=%s description=%s last_active=%s coords=%s location=%s' 
                'provider=%s public_ip=%s'
                % (nodeid, name, description, last_active, coords, location,
                       provider, public_ip))

        state, message = self.db.update_node(
            nodeid=nodeid, 
            name=name, 
            description=description, 
            last_active=last_active,
            coords=coords,
            location=location,
            provider=provider,
            public_ip=public_ip)
        
        result = {
            'state': state,
            'message': message
        }
        return pb2.message_update_node_response(**result)


    @CheckToken(pb2.message_set_scavenger_status_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def set_scavenger_status(self, request, context):
        """Set scavenger status (whether ``True`` or ``Fasle``) for a node.
        Only a user with role ``ADMIN`` and the user with ``userid==nodeid`` are 
        allowed to invoke the rpc. 
            
        :param request: gRPC request message for ``set_scavenger_status``
        :type request: :class:`pb2.message_set_scavenger_status`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``state`` is set to ``0``, otherwise 
            it is set to ``1``. In both cases, ``message`` contains details 
            about the status of the request.
        :rtype: :class:`pb2.message_set_scavenger_status_response`
        """

        nodeid = request.nodeid 
        scavenger_mode_active = request.scavenger_mode_active
        userid = context.creds_userid
        role = context.creds_role
        role_name = LeotestUserRoles(role).name
        log.info("[set_scavenger_status] nodeid=%s userid=%s role=%s scavenger_mode_active=%s" 
                            % (nodeid, userid, role_name, scavenger_mode_active))


        if (nodeid==userid and role == LeotestUserRoles.NODE.value) or role == LeotestUserRoles.ADMIN.value: 
            state, message = self.db.set_scavenger_status(nodeid, scavenger_mode_active)
        
        else:
            state = 1
            message = "permission denied: userid=%s and role=%s" % (userid, role_name)

        result = {
            'state': state,
            'message': message
        }
        return pb2.message_set_scavenger_status_response(**result)
    
    @CheckToken(pb2.message_get_scavenger_status_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def get_scavenger_status(self, request, context):
        """Get scavenger status (whether ``Active`` or ``Inactive``) for a node.
        Only a user with role ``ADMIN`` and the user with ``userid==nodeid`` are 
        allowed to invoke the rpc. 
            
        :param request: gRPC request message for ``get_scavenger_status``
        :type request: :class:`pb2.message_get_scavenger_status`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: Tf the node is found, ``found`` is set to ``True`` and 
            ``scavenger_mode_active`` is set to the status of the scavenger mode
            of the node. If scavenger mode is active, ``scavenger_mode_active=True``,
            otherwise ``scavenger_mode_active=False``. If the node is not found,
            ``found=False`` and ``scavenger_mode_active`` is undefined.
        :rtype: :class:`pb2.message_get_scavenger_status_response`
        """

        nodeid = request.nodeid 
        log.info("[get_scavenger_status] nodeid=%s" % (nodeid))
        ret = self.db.get_scavenger_status(nodeid)

        log.info(ret)

        if ret:
            result = {
                'found': True,
                'scavenger_mode_active': ret
            }
        else:
            result = {
                'found': False,
                'scavenger_mode_active': False
            }

        return pb2.message_get_scavenger_status_response(**result)

    @CheckToken(pb2.message_schedule_task_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def schedule_task(self, request, context):
        """Schedule a task on a node. This rpc is not to be exposed to the user 
        and is only used internally by the testbed. An example scenario is setting 
        up a server for jobs that require a receiver that is located on another 
        node that is a part of the testbed. In such a scenario, the client node
        schedules a task on the node with ``nodeid==job.server`` and polls it to
        check if the server has been deployed or not. 
            
        :param request: gRPC request message for ``schedule_task``
        :type request: :class:`pb2.message_schedule_task`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: Tf the node is found, ``found`` is set to ``True`` and 
            ``scavenger_mode_active`` is set to the status of the scavenger mode
            of the node. If scavenger mode is active, ``scavenger_mode_active=True``,
            otherwise ``scavenger_mode_active=False``. If the node is not found,
            ``found=False`` and ``scavenger_mode_active`` is undefined
        :rtype: :class:`pb2.message_schedule_task_response`

        .. todo::
            Allow only users with role ``NODE`` to be able to schedule tasks.
        """

        _type = request.task.type
        taskid = request.task.taskid
        runid = request.task.runid 
        jobid = request.task.jobid
        nodeid = request.task.nodeid 
        # status = request.task.status 
        status = 1 # TASK_SCHEDULED
        ttl_secs = request.task.ttl_secs 
        # taskid, runid, jobid, nodeid, task_type
        task = LeotestTask(taskid=taskid, 
                            runid=runid, 
                            jobid=jobid, 
                            nodeid=nodeid, 
                            task_type=_type,
                            ttl_secs=ttl_secs)
        task.set_status(status)

        log.info("[schedule_task] type=%s taskid=%s runid=%s jobid=%s nodeid=%s ttl_secs=%s" 
                        % (_type, taskid, runid, jobid, nodeid, ttl_secs))
        state, message = self.db.schedule_task(task)

        result = {
            'state': state,
            'message': message
        }
        return pb2.message_schedule_task_response(**result)
    
    @CheckToken(pb2.message_get_tasks_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token")     
    def get_tasks(self, request, context):
        """Get tasks after filtering them on certain parameters. The parameters
        that can be specified are: ``taskid``, ``runid``, ``jobid``, ``nodeid``.
        All parameters are optional.
            
        :param request: gRPC request message for ``get_tasks``
        :type request: :class:`pb2.message_get_tasks`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``tasks`` contains the lists of 
            tasks
        :rtype: :class:`pb2.message_get_tasks_response`

        .. todo::
            Allow only users with role ``NODE`` to be able to get tasks.
        """

        taskid = request.taskid if request.HasField('taskid') else None
        runid = request.runid if request.HasField('runid') else None
        jobid = request.jobid if request.HasField('jobid') else None
        nodeid = request.nodeid if request.HasField('nodeid') else None

        log.info("[get_tasks] taskid=%s runid=%s jobid=%s nodeid=%s" 
                        % (taskid, runid, jobid, nodeid))


        tasks = self.db.get_tasks(taskid=taskid, 
                                    runid=runid, 
                                    jobid=jobid, 
                                    nodeid=nodeid)

        log.info('tasks=%s' % str(tasks))
        result = {'tasks': tasks}
        return pb2.message_get_tasks_response(**result)

    @CheckToken(pb2.message_update_task_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def update_task(self, request, context):
        """Update task status.
            
        :param request: gRPC request message for ``update_task``
        :type request: :class:`pb2.message_update_task`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``state`` is set to ``0``, otherwise 
            it is set to ``1``. In both cases, ``message`` contains details 
            about the status of the request
        :rtype: :class:`pb2.message_update_task_response`

        .. todo::
            Allow only users with role ``NODE`` to be able to update tasks.
        """

        taskid = request.taskid 
        status = request.status 
        log.info("[update_task] taskid=%s status=%s" % (taskid, status))
        state, message = self.db.update_task(taskid=taskid, status=status)
        result = {
            'state': state,
            'message': message
        }

        return pb2.message_update_task_response(**result)
    

    @CheckToken(pb2.message_kernel_access_response, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def kernel_access(self, request, context):
        """Verify whether a userid has access to kernel services. This rpc 
        is invoked by the kernel service running on the nodes to verify if the 
        user who scheduled the job has privileges to modify/reconfigure kernel
        configuration. Usually users with roles ``USER_PRIV``, ``NODE_PRIV``, and
        ``ADMIN`` have access to kernel service. Only users with roles ``NODE`` and 
        ``ADMIN`` hare allowed to invoke the rpc.
            
        :param request: gRPC request message for ``kernel_access``
        :type request: :class:`pb2.message_kernel_access`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``state`` is set to ``0``, otherwise 
            it is set to ``1``. In both cases, ``message`` contains details 
            about the status of the request.
        :rtype: :class:`pb2.message_kernel_access_response`
        """

        _userid = context.creds_userid
        _role = context.creds_role
        _role_name = LeotestUserRoles(_role).name
        verify_userid = request.userid
        log.info("[kernel_access] userid=%s role=%s verify_userid=%s" 
                                               % (_userid, _role_name, verify_userid))

        state = 1 
        message = 'verification failed: unknown reason'

        if (_role == LeotestUserRoles.NODE.value)\
            or (_role == LeotestUserRoles.ADMIN.value): 


            # get role for verify_userid
            user = self.db.get_user(verify_userid)
            if user:
                log.info("[kernel_access] verify_userid=%s role=%s" 
                                        % (verify_userid, user.role))
                if (user.role == LeotestUserRoles.USER_PRIV.value)\
                    or (user.role == LeotestUserRoles.NODE_PRIV.value)\
                    or (user.role == LeotestUserRoles.ADMIN.value):

                    state = 0
                    message = 'userid=%s role=%s has access to kernel services'\
                                % (verify_userid, LeotestUserRoles(user.role).name)
                
                else:
                    state = 1 
                    message = 'userid=%s role=%s does not have access to kernel services (higher access level required)'\
                                % (verify_userid, LeotestUserRoles(user.role).name)
            else:
                state = 1 
                message = 'userid=%s does not exist' % verify_userid
        else:
            state = 1
            message = "permission denied: userid=%s and role=%s" % (_userid, _role_name)

        result = {
            'state': state,
            'message': message
        }
        return pb2.message_kernel_access_response(**result)
    
    @CheckToken(pb2.message_get_scheduled_runs, 
                grpc.StatusCode.UNAUTHENTICATED, 
                "Invalid token") 
    def get_scheduled_runs(self, request, context):
        """Get a list of runs scheduled in a specific time range in future. 
        This rpc returns a list of runs (or ``events``) in the format that can
        be used with the react's fullCalender API. As such, it helps to populate
        the calender in the web UI for a better visualization of available 
        time slots for job scheduling. 
            
        :param request: gRPC request message for ``get_scheduled_runs``
        :type request: :class:`pb2.message_get_scheduled_runs`
        :param context: gRPC context
        :type context: Refer to gRPC documentation
        :return: If the request is successful, ``state`` is set to ``0``, otherwise 
            it is set to ``1``. In both cases, ``message`` contains details 
            about the status of the request. When the request is successful, 
            ``events`` caontains the list of all the events in the specified 
            time range.
        :rtype: :class:`pb2.message_get_scheduled_runs_response`
        """
        nodeid = request.nodeid 
        start = request.start 
        end = request.end

        job_list = []
        exists, jobs = self.db.get_jobs_by_nodeid(nodeid=nodeid)
        if exists:
            for job in jobs:
                _type = pb2.job_type.Value(job.type.upper())

                if job.type.lower() == 'cron':
                    res = {
                        'jobid': job.jobid,
                        'userid': job.userid,
                        'type': job.type.lower(),
                        'params': job.job_params,
                        'schedule': job.get_cron_string(),
                        'start': job.start_date, 
                        'end': job.end_date,
                        'length': job.length_secs,
                        'overhead': job.overhead,
                        'trigger': job.trigger,
                        'server': job.server
                    }

                elif job.type.lower() == 'atq':
                    res = {
                        'jobid': job.jobid,
                        'userid': job.userid,
                        'type': job.type.lower(),
                        'params': job.job_params,
                        'schedule': '',
                        'start': job.start_date, 
                        'end': job.end_date,
                        'length': job.length_secs,
                        'overhead': job.overhead,
                        'trigger': job.trigger,
                        'server': job.server
                    }

                job_list.append(res)

        state, message, run_events = get_event_list_from_job_list(job_list, start, end)
        
        result = {
            'state': state,
            'message': message,
            'events': run_events 
        }
        return pb2.message_get_scheduled_runs_response(**result)

class LeotestOrchestrator:
    """Wrapper class that bootstraps the gRPC service of the 
    LEOScope's orchestartor.
    """
    def __init__(self,
            grpc_hostname='localhost', 
            grpc_port=50051, 
            admin_access_token='',
            jwt_secret='',
            jwt_algo='',
            grpc_max_workers=10,
            db_server='localhost',
            db_port=27017,
            db_name='leotest'):
        """
        :param grpc_hostname: Hostname or ip address on which to listen to 
            for in-coming rpc requests, defaults to 'localhost'
        :type grpc_hostname: str, optional
        :param grpc_port: Port number on which to listen to for in-coming 
            rpc requests, defaults to 50051
        :type grpc_port: int, optional
        :param admin_access_token: Access token to use for registering the ``admin``
            user on LEOScope, defaults to ''
        :type admin_access_token: str, optional
        :param jwt_secret: Secret to use for verifying Java Web Tokens (JWT) used 
            by the web UI, defaults to ''
        :type jwt_secret: str, optional
        :param jwt_algo: Algorithm to use for verufying the JWTs used by the 
            web UI, defaults to ''
        :type jwt_algo: str, optional
        :param grpc_max_workers: Number of workers for the gRPC server, defaults to 10
        :type grpc_max_workers: int, optional
        :param db_server: Hostname of the orchestrator's MongoDB datastore, defaults to 'localhost'
        :type db_server: str, optional
        :param db_port: Port number of orchestrator's MongoDB datastore , defaults to 27017
        :type db_port: int, optional
        :param db_name: Name of the database (or document collection) on the datastore in which
            to store LEOScope's data (nodes, users, job schedules, etc..), defaults to 'leotest'
        :type db_name: str, optional
        """

        self.grpc_service_instance = LeotestOrchestratorGrpc(
            db_server=db_server, db_port=db_port, db_name=db_name, 
            admin_access_token=admin_access_token, jwt_secret=jwt_secret,
            jwt_algo=jwt_algo)
        self.grpc_max_workers = grpc_max_workers
        self.grpc_hostname = grpc_hostname 
        self.grpc_port = grpc_port

    
    def start_grpc_service(self):
        """Bootsraps the gRPC service of the orchestartor.
        """
        log.info("starting grpc service")
        grpc_service = grpc.server(
                        futures.ThreadPoolExecutor(
                                max_workers=self.grpc_max_workers))
        pb2_grpc.add_LeotestOrchestratorServicer_to_server(
                        self.grpc_service_instance, grpc_service)
        
        # grpc_service.add_insecure_port('%s:%d' % (self.grpc_hostname, 
        #                                             self.grpc_port))
        # grpc_service.start()
        # grpc_service.wait_for_termination()

        with open('certs/server.key', 'rb') as f:
            private_key = f.read()
        with open('certs/server.crt', 'rb') as f:
            certificate_chain = f.read()

        server_credentials = grpc.ssl_server_credentials(((private_key, certificate_chain,),))
        grpc_service.add_secure_port('[::]:50051', server_credentials)
        grpc_service.start()
        grpc_service.wait_for_termination()
    
    
    def start(self):
        """Wrapper around :meth:`LeotestOrchestrator.start_grpc_service`
        """
        log.info("starting controller services") 
        self.start_grpc_service()
