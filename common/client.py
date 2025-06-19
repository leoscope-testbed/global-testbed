import os
import grpc
import json
import shutil
import tempfile
import logging 

import common.leotest_pb2_grpc as pb2_grpc 
import common.leotest_pb2 as pb2
from common.utils import StorageDirectoryClient, time_now

from tenacity import Retrying, retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from google.protobuf.json_format import Parse
from google.protobuf.json_format import MessageToDict
from dateutil.parser import parse as datetimeParse 


logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
log = logging.getLogger(__name__)


class LeotestClient:

    def __init__(self,
        grpc_hostname='localhost', 
        grpc_port=50051, userid='admin', access_token='', jwt_access_token='',
        conn_retry_num=10, conn_retry_wait=3, timeout=5):

        self.grpc_stub = None
        self.grpc_hostname = grpc_hostname 
        self.grpc_port = grpc_port 
        self.userid = userid 
        self.access_token = access_token 
        self.jwt_access_token = jwt_access_token
        self.conn_retry_num = conn_retry_num
        self.conn_retry_wait = conn_retry_wait
        self.timeout = timeout

        self.init_grpc_client()


    def _retry(self, forever=True): 
        # TODO: setting forever=True is a workaround to avoid node exits.
        # If any request fails after retries have expired, an exception is generated,
        # causing the node to exit. Ideally, for non-dertrimental requests, the 
        # thread should just exit without generating an exception. 
        # Enable better error handling in future to allow only important requests 
        # to try forever (like heartbeat and job gets), and for rest of the requests, exit silently 
        # after retries have been used. 
        
        def after_attempt(retry_state):
            log.warning("gRPC error (attempt %d): %s", 
                        retry_state.attempt_number,
                        retry_state.outcome.exception())
            
            # Refresh gRPC channel. 
            # TODO: 1. refresh only when DEADLINE has exceeded.
            # TODO: 2. implement KeepAlive for long lived connections
            self.init_grpc_client()

        if not forever:
            return Retrying(
                stop=stop_after_attempt(self.conn_retry_num),
                wait=wait_fixed(self.conn_retry_wait),
                retry=retry_if_exception_type((grpc.RpcError)),
                after=after_attempt)
        else:
            return Retrying(
                wait=wait_fixed(self.conn_retry_wait),
                retry=retry_if_exception_type((grpc.RpcError)),
                after=after_attempt)
    

    def init_grpc_client(self):
        
        for attempt in self._retry():
            with attempt:
                # instantiate a channel 
                # channel = grpc.insecure_channel(
                #     '{}:{}'.format(self.grpc_hostname, self.grpc_port))

                with open('certs/server.crt', 'rb') as f:
                    trusted_certs = f.read()

                credentials = grpc.ssl_channel_credentials(root_certificates=trusted_certs)
                # make sure that all headers are in lowecase, otherwise grpc throws an exception
                call_credentials = grpc.metadata_call_credentials(
                    lambda context, callback: callback((
                        ("x-leotest-access-token", self.access_token),
                        ("x-leotest-userid", self.userid),
                        ('x-leotest-jwt-access-token', self.jwt_access_token)
                    ), None))
                # use this if you want standard "Authorization" header
                #call_credentials = grpc.access_token_call_credentials("test_access_token")

                cert_cn = "localhost" # or parse it out of the cert data
                options = (('grpc.ssl_target_name_override', cert_cn,),)

                composite_credentials = grpc.composite_channel_credentials(credentials, call_credentials)
                channel = grpc.secure_channel('{}:{}'.format(self.grpc_hostname, self.grpc_port), 
                                          composite_credentials, options=options)

                # bind the client and teh server 
                self.grpc_stub = pb2_grpc.LeotestOrchestratorStub(channel)

    def send_heartbeat(self, nodeid):
        """ Send heartbeat gRPC API sends regular messages to update it's status.
        """        
        for attempt in self._retry():
            with attempt:
                log.info('sending heartbeat')
                message = pb2.message_heartbeat(nodeid=nodeid)
                return self.grpc_stub.report_heartbeat(message, timeout=self.timeout)

    def update_config(self, config_json):
        """Send a request to update the global configuration.
        This method sends a request to update the global configuration.
        """        
        for attempt in self._retry():
            with attempt:
                log.info('sending request to update global config')
                print(config_json)
                config = Parse(config_json, pb2.settings())
                message = pb2.message_update_global_config(config=config)
                return self.grpc_stub.update_global_config(message, timeout=self.timeout)
    
    def get_config(self):
        """Send a request to get the latest the global configuration.
        """        
        for attempt in self._retry():
            with attempt:
                log.info('sending request to get global config')
                message = pb2.message_get_global_config()
                return self.grpc_stub.get_global_config(message, timeout=self.timeout)

    def register_user(self, id, name, role, team):
        """Send a request to register a user with 4 parameters i.e. id, name, role and team.
        """        
        for attempt in self._retry():
            with attempt:
                log.info('sending request to register user (id=%s,name=%s,role=%s,team=%s)' 
                        % (id, name, role, team))

                roleid = pb2.user_roles.Value(role.upper())
                message = pb2.message_register_user(
                                id=id, 
                                name=name,
                                role=roleid,
                                team=team)
                return self.grpc_stub.register_user(message, timeout=self.timeout)
    
    def get_user(self, id):
        """Send a request to get user details with the given user id.
        """        
        for attempt in self._retry():
            with attempt:
                log.info('sending request to get user (id=%s)' % (id))
                message = pb2.message_get_user(userid=id)
                return self.grpc_stub.get_user(message, timeout=self.timeout)

    def modify_user(self, id, name, role, team):
        """Send a request to modify user details with the given user id.
        """    
        for attempt in self._retry():
            with attempt:
                log.info('sending request to modify user (id=%s) with params (name=%s,role=%s,team=%s)' 
                        % (id, name, role, team))

                roleid = pb2.user_roles.Value(role.upper())
                message = pb2.message_modify_user(
                                id=id, 
                                name=name,
                                role=roleid,
                                team=team)
                return self.grpc_stub.modify_user(message, timeout=self.timeout)
    
    def delete_user(self, id):
        """Send a request to delete user with the given user id.
        """    
        for attempt in self._retry():
            with attempt:
                log.info('sending request to delete user (id=%s)' % (id))
                message = pb2.message_delete_user(userid=id)
                return self.grpc_stub.delete_user(message, timeout=self.timeout)


    def schedule_job(self, jobid, nodeid, type_name, 
                        params_mode, params_deploy, 
                        params_execute, params_finish, schedule, 
                        start_date, end_date, length, overhead, server=None, trigger=None,
                        # global_config = "global_config.json", 
                        experiment_config = ""):
        """Send a request to schedule a job at a node with the given nodeid, according to the given schedule and parameters.

        :param jobid: Job ID of the job to be scheduled.
        :param nodeid: Node ID of the node where the experiment is to be deployed.
        :param type_name: Job type, Cron or atq.
        :param params_mode: Not used now, can be ignored.
        :param params_deploy: Not used now, can be ignored.
        :param params_execute: Not used now, can be ignored.
        :param params_finish: Not used now, can be ignored.
        :param schedule: crontab string for jobs with type_name 'cron', or can be left blank.
        :param start_date: Start datetime of the experiment.
        :param end_date: End datetime of the experiment
        :param length: Duration of the experiment in seconds.
        :param overhead:Overhead vs no-overhead experiment.
        :param server: node id of the server used for experimentation, defaults to None
        :param trigger: Trigger expression for running experiment, defaults to None, optional
        :param experiment_config: Config file specific to the experiment deployed, defaults to "", optional
        :return: gRPC response.
        """        
        log.info("[schedule_job] jobid=%s nodeid=%s type=%s params" 
                    "\{mode=%s deploy=%s execute=%s finish=%s\} schedule=%s"
                    "start_date=%s end_date=%s length=%d overhead=%s server=%s trigger=%s experiment_config=%s"
                    % (jobid, nodeid, type_name, params_mode, params_deploy, 
                        params_execute, params_finish, schedule, 
                        start_date, end_date, length, overhead, server, trigger, experiment_config))
        
        
        print("experiment config is ***", experiment_config)


        log.info("experiment configs are *** %s", experiment_config)
                    
        
        for attempt in self._retry():
            with attempt:
                # # open global config: 
                # with open(global_config, mode='r') as f:
                #     config = json.loads(f.read())
                
                # connection_string = config['datastore']['blob']['connectionString']
                # container = config['datastore']['blob']['container']
                # remote_path = os.path.join(
                #         config['datastore']['blob']['artifactPath'], nodeid, jobid)

                # experiment_args = {
                #     'schedule': schedule,
                #     'start_date': start_date,
                #     'end_date': end_date,
                #     'length': length,
                #     'overhead': overhead,
                #     'server': server,
                #     'trigger': trigger
                # }

                # with tempfile.TemporaryDirectory() as tmpdir:
                #     log.info('created temporary directory %s' % (tmpdir))
                #     src = experiment_config
                #     dst = os.path.join(tmpdir, 'experiment-config.yaml')
                #     shutil.copy(src, dst)
                #     # upload config files to blob storage 
                #     log.info('uploading config to blob storage %s --> (%s:%s)' 
                #                     % (experiment_config, container, remote_path))
                    
                #     upload_file(connection_string, 
                #                 container, dst, remote_path)

                #     exp_args_file = os.path.join(tmpdir, "experiment-args.json")
                #     with open(exp_args_file, "w") as write:
                #         json.dump(experiment_args , write)
                    
                #     upload_file(connection_string, 
                #                 container, exp_args_file, remote_path)                    

                _type = pb2.job_type.Value(type_name.upper())
                log.info("Scheduling job on nodeis : %s" %(nodeid))
                message = pb2.message_schedule_job(
                    id = jobid,
                    nodeid = nodeid,
                    type = _type,
                    params = {
                        'mode': params_mode,
                        'deploy': params_deploy,
                        'execute': params_execute,
                        'finish': params_finish
                    },
                    schedule = schedule, 
                    start_date = start_date,
                    end_date = end_date, 
                    length_secs = length,
                    overhead = overhead,
                    trigger = trigger,
                    config = experiment_config)
                
                if server:
                    message.server = server 
        
                return self.grpc_stub.schedule_job(message, timeout=self.timeout)
    
    def get_job_by_id(self, jobid):
        """Send a request to get job details for the given Job ID.
        """
        for attempt in self._retry():
            with attempt:
                log.info('sending request to get job by jobid (jobid=%s)' % (jobid))
                message = pb2.message_get_job_by_id(jobid=jobid)
                return self.grpc_stub.get_job_by_id(message, timeout=self.timeout)

    def get_jobs_by_userid(self, userid):
        """Send a request to get job details deployed by a user with the given user ID.
        """
        # since this is used by the main node loop, set forever=True
        for attempt in self._retry(forever=True):
            with attempt:
                log.info('sending request to get jobs by userid (userid=%s)' % (userid))
                message = pb2.message_get_jobs_by_userid(userid=userid)
                return self.grpc_stub.get_jobs_by_userid(message, timeout=self.timeout)    

    def get_jobs_by_nodeid(self, nodeid):
        """Send a request to get job details deployed on the node with the given node ID.
        """
        # since this is used by the main node loop, set forever=True
        for attempt in self._retry(forever=True):
            with attempt:
                log.info('sending request to get jobs by nodeid (nodeid=%s)' % (nodeid))
                message = pb2.message_get_jobs_by_nodeid(nodeid=nodeid)
                return self.grpc_stub.get_jobs_by_nodeid(message, timeout=self.timeout)
    
    def delete_job_by_id(self, jobid):
        """Send a request to delete job details for the given Job ID.
        """
        for attempt in self._retry():
            with attempt:
                log.info('sending request to delete job by jobid (jobid=%s)' % (jobid))
                message = pb2.message_delete_job_by_id(jobid=jobid)
                return self.grpc_stub.delete_job_by_id(message, timeout=self.timeout)


    def delete_jobs_by_nodeid(self, nodeid):
        """Send a request to delete job details for the jobs with the given node ID.
        """
        for attempt in self._retry():
            with attempt:
                log.info('sending request to delete jobs by nodeid (nodeid=%s)' % (nodeid))
                message = pb2.message_delete_jobs_by_nodeid(nodeid=nodeid)
                return self.grpc_stub.delete_jobs_by_nodeid(message, timeout=self.timeout)

    def reschedule_job_nearest(self, jobid, starttime, endtime):
        """request for job reschedule"""
        for attempt in self._retry():
            with attempt:
                message = pb2.message_reschedule_job()
                message.jobid = jobid 
                message.starttime = starttime 
                message.endtime = endtime 
                return self.grpc_stub.reschedule_job_nearest(message, timeout=self.timeout)

    def update_run(self, runid, jobid, nodeid, userid, start_time, 
                            status, status_message, 
                            last_updated=None, end_time=None, blob_url=''):
        
        if not last_updated:
            last_updated = str(time_now())
        
        if not end_time:
            end_time = start_time

        for attempt in self._retry():
            with attempt:
                log.info('sending request to update run status (jobid=%s)' % (jobid))
                run = {
                    'runid': runid, 
                    'jobid': jobid,
                    'nodeid': nodeid, 
                    'userid': userid, 
                    'start_time': start_time,
                    'end_time': end_time,
                    'last_updated': last_updated,
                    'blob_url': blob_url, 
                    'status': status,
                    'status_message': status_message
                }
                message = pb2.message_update_run(run=run)
                return self.grpc_stub.update_run(message, timeout=self.timeout)
    

    def get_runs(self, userid=None, runid=None, jobid=None, nodeid=None, time_range=None, limit=None):
        for attempt in self._retry():
            with attempt:
                log.info('sending request to fetch runs')
                message = pb2.message_get_runs()

                if userid:
                    message.userid = userid
                
                if runid:
                    message.runid = runid

                if jobid:
                    message.jobid = jobid
                
                if nodeid:
                    message.nodeid = nodeid

                if time_range:
                    message.time_range.start = time_range['start']
                    message.time_range.end = time_range['end']
                
                if limit:
                    message.limit = limit

                return self.grpc_stub.get_runs(message, timeout=self.timeout)
    
    def download_runs(self, 
                        local_path,
                        runid=None, 
                        jobid=None, 
                        nodeid=None, 
                        time_range=None, 
                        limit=None,
                        global_config = "global_config.json"):

        for attempt in self._retry():
            with attempt:
                # open global config: 
                with open(global_config, mode='r') as f:
                    config = json.loads(f.read())
                
                connection_string = config['datastore']['blob']['connectionString']
                container = config['datastore']['blob']['container']
                artifact_path = config['datastore']['blob']['artifactPath']
                blob_storage = StorageDirectoryClient(connection_string, container)

                res = self.get_runs(runid=runid, 
                    jobid=jobid, 
                    nodeid=nodeid, 
                    time_range=time_range, 
                    limit=limit)

                # print(MessageToDict(res))
                for run in MessageToDict(res)['runs']:
                    # construct remote path: jobs/nodeid/jobid/year/month/day/runid/
                    # local path: jobs/
                    nodeid = run['nodeid']
                    jobid = run['jobid']
                    runid = run['runid']
                    start_time = datetimeParse(run['startTime'])
                    year = str(start_time.year) 
                    month = str(start_time.month) 
                    day = str(start_time.day) 

                    remote_path = os.path.join(
                        artifact_path,
                        nodeid,
                        jobid,
                        year,
                        month,
                        day,
                        runid
                    )
                    
                    exists = blob_storage.ls_files(remote_path, recursive=True)
                    if not exists:
                        log.info("Experiment artifacts do not exist, skipping: "
                            "runid=%s remote_path=%s local_path=%s"
                            % (runid, remote_path, local_path))
                        
                        exp_folder = os.path.join(local_path, runid)
                        try:
                            os.makedirs(exp_folder)
                        except Exception as e:
                            print(e)

                    else:
                        log.info("downloading experiment artifacts: "
                                "runid=%s remote_path=%s local_path=%s"
                                % (runid, remote_path, local_path))
                        blob_storage.download(remote_path, local_path)
    
    def register_node(self, nodeid, name, description, coords, location, provider='starlink'):
        """register node"""

        log.info('sending request to register node %s' % nodeid)
        node = {
            'nodeid': nodeid,
            'name': name, 
            'description': description, 
            'coords': coords,
            'location': location, 
            'provider': provider
        }
        message = pb2.message_register_node(node=node)
        return self.grpc_stub.register_node(message, timeout=self.timeout)

    
    def delete_node(self, nodeid, delete_jobs = False):
        """delete a node with a given nodeid"""

        log.info('sending request to delete node with nodeid=%s delete_jobs=%s' 
                                            % (nodeid, delete_jobs))

        message = pb2.message_delete_node(nodeid=nodeid, deleteJobs=delete_jobs)
        return self.grpc_stub.delete_node(message, timeout=self.timeout)

    def get_nodes(self, nodeid=None, location=None, name=None, provider=None, 
                        active=None, activeThres=None):
        """get nodes"""

        msg = 'sending request to fetch nodes with '

        message = pb2.message_get_nodes()
        if nodeid:
            message.nodeid = nodeid
            msg += 'nodeid=%s ' % nodeid
        
        if location:
            message.location = location
            msg += 'location=%s ' % location
        
        if name:
            message.name = name
            msg += 'name=%s ' % name
    
        if provider:
            message.provider = provider
            msg += 'provider=%s ' % provider
        
        if active:
            message.active = active
            msg += 'active=%s ' % active
        
        if activeThres:
            message.activeThres = activeThres
            msg += 'activeThres=%s ' % activeThres

        log.info(msg)
        return self.grpc_stub.get_nodes(message, timeout=self.timeout)
    
    def update_node(self, nodeid, name=None, description=None, last_active=None,
        coords=None, location=None, provider=None, public_ip=None):
        """update node"""

        msg = 'sending request to update node=%s with ' % nodeid

        message = pb2.message_update_node()

        message.nodeid = nodeid
        msg += 'nodeid=%s ' % nodeid
        
        if name:
            message.name = name
            msg += 'name=%s ' % name
    
        if description:
            message.description = description
            msg += 'description=%s ' % description

        if last_active:
            message.last_active = last_active
            msg += 'last_active=%s ' % last_active

        if coords:
            message.coords = coords
            msg += 'coords=%s ' % coords

        if location:
            message.location = location
            msg += 'location=%s ' % location

        if provider:
            message.provider = provider
            msg += 'provider=%s ' % provider
        
        if public_ip:
            message.public_ip = public_ip
            msg += 'public_ip=%s ' % public_ip

        log.info(msg)
        return self.grpc_stub.update_node(message, timeout=self.timeout)

    def set_scavenger_status(self, nodeid, scavenger_mode_active):
        """set scavenger mode"""
    
        message = pb2.message_set_scavenger_status()
        message.nodeid = nodeid
        message.scavenger_mode_active = scavenger_mode_active

        log.info('sending request to fetch scavenger mode status;' 
           'nodeid=%s scavenger_mode_active=%s' % (nodeid, scavenger_mode_active))

        return self.grpc_stub.set_scavenger_status(message, timeout=self.timeout)
    
    def get_scavenger_status(self, nodeid):
        """get scavenger mode status"""

        message = pb2.message_get_scavenger_status()
        message.nodeid = nodeid 
        log.info('sending request to fetch scavenger mode state; nodeid=%s'
                                       % (nodeid))
        
        return self.grpc_stub.get_scavenger_status(message, timeout=self.timeout)

    def schedule_task(self, taskid, runid, jobid, nodeid, _type, ttl_secs=300):
        
        message = pb2.message_schedule_task()
        message.task.type = pb2.task_type.Value(_type)
        message.task.taskid = taskid 
        message.task.runid = runid 
        message.task.jobid = jobid 
        message.task.nodeid = nodeid  
        message.task.ttl_secs = ttl_secs

        log.info('sending message to schedule task with '
                'type=%s taskid=%s runid=%s jobid=%s nodeid=%s ttl_secs=%s' 
                % (_type, taskid, runid, jobid, nodeid, ttl_secs)
        )
        return self.grpc_stub.schedule_task(message, timeout=self.timeout)

    def get_tasks(self, taskid=None, runid=None, jobid=None, nodeid=None):
        
        msg = 'sending request to fetch nodes with '
        message = pb2.message_get_tasks()

        if taskid:
            message.taskid = taskid
            msg += 'taskid=%s ' % taskid
        
        if runid:
            message.runid = runid
            msg += 'runid=%s ' % runid
        
        if jobid:
            message.jobid = jobid
            msg += 'jobid=%s ' % jobid

        if nodeid:
            message.nodeid = nodeid
            msg += 'nodeid=%s ' % nodeid
        
        log.info(msg)
        return self.grpc_stub.get_tasks(message, timeout=self.timeout)
    
    def update_task(self, taskid, status='TASK_COMPLETE'):

        log.info('sending request to update task with taskid=%s status=%s'
                % (taskid, status))
        
        message = pb2.message_update_task()
        message.taskid = taskid 
        message.status = pb2.task_status.Value(status) 

        return self.grpc_stub.update_task(message, timeout=self.timeout)
    
    def kernel_access(self, userid):
        log.info('sending request for kernel access verification; userid=%s' % userid)
        message = pb2.message_kernel_access()
        message.userid = userid 
        return self.grpc_stub.kernel_access(message, timeout=self.timeout)

    def get_scheduled_runs(self, nodeid, start, end):
        log.info('sending request to fetch scheduled runs; nodeid=%s start=%s end=%s' 
                       % (nodeid, start, end))    

        message = pb2.message_get_scheduled_runs()
        message.nodeid = nodeid 
        message.start = start 
        message.end = end 
        return self.grpc_stub.get_scheduled_runs(message, timeout=self.timeout)



