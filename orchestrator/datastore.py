'''
Contains code for interaction between the orchestrator and the Mongo database.
Database contains information about the users, nodes, job schedules, runs (status of the jobs, link to the artifacts), tasks and the testbed configs.
'''


from pymongo import ASCENDING, DESCENDING, DeleteOne, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from bson.binary import Binary
from typing import List
import logging 
import json

from dateutil.parser import parse as datetimeParse
import datetime 
from common.job import LeotestJob, LeotestTask, LeotestJobCron, LeotestJobAtq,\
                                                        LeotestRun
from common.user import LeotestUser, LeotestUserRoles
from common.node import LeotestNode
from common.utils import time_now

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
log = logging.getLogger(__name__)

class LeotestDatastoreMongo:
    PENDING_REGISTRATION_STATES = {"pending_signup", "signup_link_sent"}

    def __init__(self, server='localhost', port=27017, database='leotest', 
                admin_access_token='leotest-access-token') -> None:
        self.client = MongoClient(server, port)
        self.database = database

        self.db = self.client.get_database(self.database)
        self._config = self.db["config"]
        self._users = self.db["users"]
        self._nodes = self.db["nodes"]
        self._jobs = self.db["jobs"]
        self._runs = self.db["runs"]
        self._tasks = self.db["tasks"]

        self._jobs.create_index('expire_at', expireAfterSeconds=0)
        self._runs.create_index([('jobid', ASCENDING),
                                ('nodeid', ASCENDING),
                                ('start_time', DESCENDING)],
                                name='run_query_index')

        self._nodes.create_index([('nodeid', ASCENDING),
                                ('last_active', DESCENDING)],
                                name='node_query_index')
        self._nodes.create_index([('owner', ASCENDING),
                                ('nodeid', ASCENDING)],
                                name='node_owner_query_index')
        
        self._tasks.create_index('expire_at', expireAfterSeconds=0)
        self._users.create_index('id', name='user_id_index')
        self._users.create_index('signup_token_hash', sparse=True,
                                name='signup_token_hash_index')
        self._users.create_index([('registration_status', ASCENDING),
                                ('access_request_status', ASCENDING)],
                                name='user_registration_status_index')
        self.admin_access_token = admin_access_token

        # create an admin user 
        admin = LeotestUser(id='admin', name='LEOScope Admin', 
                            role=LeotestUserRoles.ADMIN.value, team='LeoScope MSR-India',
                            static_access_token = self.admin_access_token, 
                            access_token = self.admin_access_token)

        log.info(admin.document())
        ret = self.delete_user(admin)
        log.info(ret)
        ret = self.add_user(admin)
        log.info(ret)

    def update_config(self, config):
        with self.client.start_session() as session:
            config["id"] = 0
            log.info("[datastore] update_config: %s", config)
            # self._config.insert_one(config, session=session)
            self._config.update_one({
                'id': 0
            }, {
                '$set': dict(config)
            },
            upsert=True, session=session)
        
        return (0, "config updated")

    def get_config(self):
        with self.client.start_session() as session:
            config = self._config.find_one({"id": 0}, 
                                        session=session)
        return (0, config)

    def _coerce_datetime(self, value, fallback=None):
        if isinstance(value, datetime.datetime):
            date_value = value
        elif value:
            date_value = datetimeParse(str(value))
        elif fallback:
            date_value = fallback
        else:
            date_value = time_now()

        if date_value.tzinfo:
            date_value = date_value.astimezone(
                datetime.timezone.utc).replace(tzinfo=None)
        return date_value.replace(microsecond=0)

    def _is_pending_user_document(self, user):
        return bool(user) and user.get(
            "registration_status") in self.PENDING_REGISTRATION_STATES

    def _is_active_user_document(self, user):
        if not user or self._is_pending_user_document(user):
            return False
        return bool(user.get("access_token") or user.get("static_access_token"))

    def _user_from_document(self, user):
        if not self._is_active_user_document(user):
            return None

        return LeotestUser(id=user.get('id', ''),
                            name=user.get('name', ''),
                            role=user.get('role', LeotestUserRoles.USER.value),
                            team=user.get('team', ''),
                            static_access_token=user.get('static_access_token', ''),
                            access_token=user.get('access_token', ''))

    def get_user_document(self, userid):
        with self.client.start_session() as session:
            return self._users.find_one({"id": userid}, session=session)

    def add_user(self, user: LeotestUser):
        """register user on leotest"""

        document = user.document()
        now = time_now().replace(microsecond=0)
        document.setdefault('registration_status', 'active')
        document.setdefault('access_request_status', 'registered')
        document.setdefault('updated_at', now)
        with self.client.start_session() as session: 
            exists = self._users.find_one({"id": document['id']}, 
                                                    session=session)
            if self._is_active_user_document(exists):
                return (1, "user with given id already exists")

            if exists:
                document.setdefault('created_at', exists.get('created_at', now))
                self._users.update_one({
                    '_id': exists['_id']
                }, {
                    '$set': document,
                    '$unset': {
                        'signup_token_hash': '',
                        'signup_token_expires_at': ''
                    }
                }, session=session)
                return (0, "user registered")
            else:
                document.setdefault('created_at', now)
                self._users.insert_one(document, session=session)
                return (0, "user registered")

    def get_user(self, userid) -> LeotestUser:
        with self.client.start_session() as session:
            user = self._users.find_one({"id": userid}, 
                                        session=session)

            return self._user_from_document(user)


    def modify_user(self, user: LeotestUser) -> None:
        """modify user on Leotest"""

        # does the user exist?
        document = user.document()
        id = document['id']
        exists = self.get_user(id)
        if not exists:
            return (1, "user with given id does not exist")

        with self.client.start_session() as session: 

            document.pop('id', None)
            if document.get('static_access_token') == '':
                document.pop('static_access_token', None)
            if document.get('access_token') == '':
                document.pop('access_token', None)
            document['updated_at'] = time_now().replace(microsecond=0)
            self._users.update_one({
                'id': id
            }, {
                '$set': document
            },
            upsert=False, session=session)

        
        return (0, "user updated")
    
    def delete_user(self, user: LeotestUser) -> None:
        """delete a user on Leotest"""

        document = user.document()
        exists = self.get_user(user.id)
        if not exists:
            return (1, "user with given id does not exist")

        with self.client.start_session() as session: 
            id = document['id']
            self._users.delete_one({'id': id}, session=session)   

        return (0, "user deleted")

    def submit_access_request(self, request_data):
        email = request_data.get('email', '').strip().lower()
        if not email:
            return (1, "email is required")
        if not request_data.get('accepted_eula'):
            return (1, "EULA acceptance is required")
        signup_token_hash = request_data.get('signup_token_hash', '').strip()
        if not signup_token_hash:
            return (1, "signup token hash is required")

        signed_at = self._coerce_datetime(request_data.get('signed_at'))
        token_created_at = self._coerce_datetime(
            request_data.get('signup_token_created_at'), signed_at)
        token_expires_at = self._coerce_datetime(
            request_data.get('signup_token_expires_at'), signed_at)
        domain = email.split('@')[-1] if '@' in email else ''
        full_name = request_data.get('full_name', '').strip()
        organisation = request_data.get('organisation', '').strip()
        request_role = request_data.get('request_role', '').strip()
        signature = request_data.get('signature', '').strip()
        pdf_filename = request_data.get('pdf_filename') or (
            "leoscope-signed-eula-%s.pdf" % email.replace('@', '_at_'))

        request_record = {
            'status': 'signup_link_sent',
            'requested_at': signed_at,
            'updated_at': signed_at,
            'full_name': full_name,
            'email': email,
            'email_domain': domain,
            'organisation': organisation,
            'role': request_role,
            'accepted_eula': bool(request_data.get('accepted_eula')),
            'signature': signature,
            'eula_version': request_data.get('eula_version', ''),
            'eula_signed_at': signed_at,
            'email_status': 'pending'
        }

        pending_user_update = {
            'id': email,
            'name': full_name,
            'team': "%s - " % organisation,
            'role': LeotestUserRoles.USER.value,
            'registration_status': 'signup_link_sent',
            'access_request_status': 'signup_link_sent',
            'access_request': request_record,
            'eula': {
                'version': request_data.get('eula_version', ''),
                'signed_at': signed_at,
                'signature': signature,
                'pdf_filename': pdf_filename,
                'pdf_content_type': 'application/pdf',
                'signed_pdf': Binary(request_data.get('signed_pdf') or b''),
            },
            'signup_token_hash': signup_token_hash,
            'signup_token_created_at': token_created_at,
            'signup_token_expires_at': token_expires_at,
            'signup_token_used_at': None,
            'updated_at': signed_at,
        }

        with self.client.start_session() as session:
            existing_user = self._users.find_one({'id': email}, session=session)
            if self._is_active_user_document(existing_user):
                return (1, "an active account already exists for this email")

            if existing_user:
                pending_user_update['role'] = existing_user.get(
                    'role', LeotestUserRoles.USER.value)
                pending_user_update['created_at'] = existing_user.get(
                    'created_at', signed_at)
                self._users.update_one({
                    '_id': existing_user['_id']
                }, {
                    '$set': pending_user_update
                }, session=session)
            else:
                pending_user_update['created_at'] = signed_at
                self._users.insert_one(pending_user_update, session=session)

        return (0, "access request recorded")

    def update_access_request_email_status(self, email, email_status, status_at):
        email = email.strip().lower()
        status_at = self._coerce_datetime(status_at)

        timestamp_field_map = {
            'sent': 'email_sent_at',
            'failed': 'email_failed_at',
        }

        update_fields = {
            'access_request.email_status': email_status,
            'access_request.updated_at': status_at,
            'updated_at': status_at,
        }
        if email_status in timestamp_field_map:
            update_fields['access_request.%s' % timestamp_field_map[email_status]] = status_at

        with self.client.start_session() as session:
            result = self._users.update_one({
                'id': email,
                'registration_status': {'$in': list(self.PENDING_REGISTRATION_STATES)}
            }, {
                '$set': update_fields
            }, session=session)

        if result.matched_count == 0:
            return (1, "pending access request not found")
        return (0, "access request email status updated")

    def get_registration_invite(self, signup_token_hash):
        with self.client.start_session() as session:
            invite = self._users.find_one({
                'signup_token_hash': signup_token_hash,
                'registration_status': {'$in': list(self.PENDING_REGISTRATION_STATES)},
                'signup_token_used_at': None,
            }, session=session)

        if not invite:
            return (1, "signup link is invalid or has already been used", None)

        expires_at = invite.get('signup_token_expires_at')
        if expires_at:
            expires_at = self._coerce_datetime(expires_at)
            invite['signup_token_expires_at'] = expires_at
            if expires_at < time_now():
                return (1, "signup link has expired", None)

        return (0, "registration invite found", invite)

    def activate_signup_user(self, signup_token_hash, email, name, team,
                             password_hash, role, signup_token_used_at):
        email = email.strip().lower()
        used_at = self._coerce_datetime(signup_token_used_at)

        with self.client.start_session() as session:
            invite = self._users.find_one({
                'signup_token_hash': signup_token_hash,
                'registration_status': {'$in': list(self.PENDING_REGISTRATION_STATES)},
                'signup_token_used_at': None,
            }, session=session)

            if not invite:
                return (1, "signup link is invalid or has already been used")

            if invite.get('id') != email:
                return (1, "signup link does not match this email address")

            expires_at = invite.get('signup_token_expires_at')
            if expires_at:
                expires_at = self._coerce_datetime(expires_at)
                if expires_at < time_now():
                    return (1, "signup link has expired")

            self._users.update_one({
                '_id': invite['_id']
            }, {
                '$set': {
                    'id': email,
                    'name': name,
                    'team': team,
                    'role': role,
                    'access_token': password_hash,
                    'static_access_token': password_hash,
                    'registration_status': 'active',
                    'access_request_status': 'registered',
                    'signup_token_used_at': used_at,
                    'updated_at': used_at
                },
                '$unset': {
                    'signup_token_hash': '',
                    'signup_token_expires_at': ''
                }
            }, session=session)

        return (0, "user registered")

    # TODO: 'nodeid' is a foreign key, check for consistency
    def add_job(self, job: LeotestJob) -> None:
        """add a job"""
        
        document = job.document()
        with self.client.start_session() as session: 
            exists = self._jobs.find_one({"id": document['id']}, 
                                                    session=session)
            if exists: 
                return (1, 'job with the given id exists')
            else:
                self._jobs.insert_one(document, session=session)
        
        return (0, "job successfully scheduled")

    def modify_job(self, job: LeotestJob) -> None:
        """modify job on Leotest"""

        document = job.document()
        with self.client.start_session() as session: 
            id = document['id']
            document.pop('id', None)
            self._jobs.update_one({
                'id': id
            }, {
                '$set': document
            },
            upsert=False, session=session)
    
    def get_job_by_id(self, jobid):
        """get a specific job using jobid"""
        with self.client.start_session() as session:
            job = self._jobs.find_one({"id": jobid}, 
                                        session=session)
            
            if job:
                if job['type'] == 'cron':
                    ret = LeotestJobCron(
                                jobid=job['id'],
                                nodeid=job['nodeid'],
                                userid=job['userid'],
                                job_params=job['params'],
                                start_date=job['start_date'],
                                end_date=job['end_date'],
                                length_secs=job['length_secs'],
                                overhead=job['overhead'],
                                server=job['server'], 
                                trigger=job['trigger'],
                                config=job['config'] if 'config' in job else "",
                                **job['schedule'])

                elif job['type'] == 'atq':
                    ret = LeotestJobAtq(
                                jobid=job['id'],
                                nodeid=job['nodeid'],
                                userid=job['userid'],
                                job_params=job['params'],
                                start_date=job['start_date'],
                                end_date=job['end_date'],
                                length_secs=job['length_secs'],
                                overhead=job['overhead'],
                                server=job['server'],
                                trigger=job['trigger'], 
                                config=job['config'] if 'config' in job else "")
                
                return (True, ret)
            else:
                return (False, None)

    def get_jobs_by_userid(self, userid):
        """get a specific job using userid"""

        with self.client.start_session() as session:

            if userid:
                res = self._jobs.find({"userid": userid}, 
                                            session=session)
            else:
                res = self._jobs.find({}, session=session)
            # TODO: res is always exists, use len(res) instead?
            jobs = []
            if res and res.explain().get("executionStats", {}).get("nReturned") > 0:
                for job in res:
                    if job['type'] == 'cron':
                        jobs.append(LeotestJobCron(
                                            jobid=job['id'],
                                            nodeid=job['nodeid'],
                                            userid=job['userid'],
                                            job_params=job['params'],
                                            start_date=job['start_date'],
                                            end_date=job['end_date'],
                                            length_secs=job['length_secs'], 
                                            overhead=job['overhead'],
                                            server=job['server'], 
                                            trigger=job['trigger'],
                                            **job['schedule']))
                    
                    elif job['type'] == 'atq':
                        jobs.append(LeotestJobAtq(
                                            jobid=job['id'],
                                            nodeid=job['nodeid'],
                                            userid=job['userid'],
                                            job_params=job['params'],
                                            start_date=job['start_date'],
                                            end_date=job['end_date'],
                                            length_secs=job['length_secs'], 
                                            overhead=job['overhead'],
                                            server=job['server'], 
                                            trigger=job['trigger']))
                return (True, jobs)
            else:
                return (False, None)

    def get_jobs_by_nodeid(self, nodeid) -> List[LeotestJob]:
        """get list of jobs scheduled on a given node"""

        with self.client.start_session() as session:
            # res = self._jobs.find({"nodeid": nodeid}, 
            #                             session=session)
            query = {'$or': [
                {"nodeid": nodeid},
                {"server": nodeid}
            ]}
            res = self._jobs.find(query, session=session)
            # TODO: res is always exists, use len(res) instead?
            jobs = []
            if res and res.explain().get("executionStats", {}).get("nReturned") > 0:
                for job in res:
                    if job['type'] == 'cron':
                        jobs.append(LeotestJobCron(
                                            jobid=job['id'],
                                            nodeid=job['nodeid'],
                                            userid=job['userid'],
                                            job_params=job['params'],
                                            start_date=job['start_date'],
                                            end_date=job['end_date'],
                                            length_secs=job['length_secs'], 
                                            overhead=job['overhead'],
                                            server=job['server'], 
                                            trigger=job['trigger'],
                                            **job['schedule']))
                    
                    elif job['type'] == 'atq':
                        jobs.append(LeotestJobAtq(
                                            jobid=job['id'],
                                            nodeid=job['nodeid'],
                                            userid=job['userid'],
                                            job_params=job['params'],
                                            start_date=job['start_date'],
                                            end_date=job['end_date'],
                                            length_secs=job['length_secs'], 
                                            overhead=job['overhead'],
                                            server=job['server'], 
                                            trigger=job['trigger']))
                return (True, jobs)
            else:
                return (False, None)
    
    def delete_job_by_id(self, jobid) -> None:
        """delete a job on Leotest"""
        # check if the job exists 
        with self.client.start_session() as session:
            exists = self._jobs.find_one({"id": jobid}, 
                                            session=session)
            if not exists:
                return (False, "job with given jobid does not exist")

            self._jobs.delete_one({'id': jobid}, session=session)
        
        return (True, "deleted job successfully")
    
    def delete_jobs_by_nodeid(self, nodeid):
        """delete all jobs on a given node"""
        with self.client.start_session() as session:
            res = self._jobs.delete_many({"nodeid": nodeid},
                                        session=session)
        return (res.deleted_count, "deleted records successfully")
    

    def update_job_date(self, jobid, start_date, end_date):
        """update a jobs start_time given jobid"""

        log.info('[datastore] updating job time: jobid=%s start_date=%s end_date=%s'
                 % (jobid, str(start_date), str(end_date)))
        with self.client.start_session() as session:
            res = self._jobs.update_one({
                'id': jobid
            }, 
            {
                '$set': {'start_date': str(start_date), 'end_date': str(end_date)}
            },
            upsert=False, session=session)
        
        return (True, "updated jobid start time successfully")

    def update_run(self, run: LeotestRun):
        """Add or update a run record in MongoDB."""

        document = run.document()
        runid = document['runid']
        log.info("[datastore] update_run runid=%s jobid=%s nodeid=%s status=%s message=%r",
                 runid, document.get('jobid'), document.get('nodeid'),
                 document.get('status'), document.get('status_message'))
        with self.client.start_session() as session:
            self._runs.update_one(
                {'runid': runid},
                {'$set': document},
                upsert=True,
                session=session)
        log.debug("[datastore] update_run committed runid=%s", runid)
        return (0, "updated run successfully")
    
    def get_runs(self, runid=None, jobid=None, nodeid=None, userid=None, time_range=None, limit=None):
        query = {}

        if runid:
            query["runid"] = runid

        if jobid:
            query["jobid"] = jobid
        
        if nodeid:
            query["nodeid"] = nodeid
        
        if userid:
            query["userid"] = userid
        
        if time_range:
            query["start_time"] = {}
            query["start_time"]["$gte"] = datetimeParse(time_range.start)
            query["start_time"]["$lte"] = datetimeParse(time_range.end)

        with self.client.start_session() as session:
            runs = []
            if limit:
                res = self._runs.find(query, session=session, limit=limit)
            else:
                res = self._runs.find(query, session=session)

            for run in res:
                run.pop('_id')
                runs.append(LeotestRun(**run))
            return runs
    
    def register_node(self, node: LeotestNode):
        """register a node"""
        document = node.document()
        now = time_now().replace(microsecond=0)
        document.setdefault('registered_at', now)
        document.setdefault('last_status_change', now)
        document.setdefault('scheduling_enabled', True)
        document.setdefault('bandwidth_limits_json', '[]')
        document.setdefault('availability_history_json', '[]')
        with self.client.start_session() as session: 
            exists = self._nodes.find_one({"nodeid": document['nodeid']}, 
                                                    session=session)
            if exists: 
                return (1, 'node with the given id exists')
            else:
                self._nodes.insert_one(document, session=session)
        
        return (0, "node successfully registered")        

    
    def delete_node(self, nodeid, delete_jobs = False):
        """delete a node"""

        exists = self.get_nodes(nodeid=nodeid, active=False)
        if not exists:
            return (1, "node with given id does not exist")

        if delete_jobs:
            self.delete_jobs_by_nodeid(nodeid)
            
        with self.client.start_session() as session: 
            self._nodes.delete_one({'nodeid': nodeid}, session=session)   
        
        return (0, "node deleted")

    def _availability_history_from_node(self, node):
        raw = node.get('availability_history_json', '[]')
        if isinstance(raw, list):
            return raw
        try:
            data = json.loads(raw or '[]')
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _append_heartbeat_history(self, node, now, active_thres=600):
        history = self._availability_history_from_node(node)
        previous_active = node.get('last_active')
        if not isinstance(previous_active, datetime.datetime):
            try:
                previous_active = datetimeParse(str(previous_active))
            except Exception:
                previous_active = None

        status_changed = False
        if not history:
            history.append({
                'state': 'online',
                'start': str(now),
                'end': str(now)
            })
            return json.dumps(history[-500:]), True

        if previous_active:
            offline_start = previous_active + datetime.timedelta(seconds=active_thres)
            if now > offline_start:
                if history[-1].get('state') == 'online':
                    history[-1]['end'] = str(offline_start)
                history.append({
                    'state': 'offline',
                    'start': str(offline_start),
                    'end': str(now)
                })
                history.append({
                    'state': 'online',
                    'start': str(now),
                    'end': str(now)
                })
                status_changed = True
            elif history[-1].get('state') == 'online':
                history[-1]['end'] = str(now)
            else:
                history.append({
                    'state': 'online',
                    'start': str(now),
                    'end': str(now)
                })
                status_changed = True
        else:
            history.append({
                'state': 'online',
                'start': str(now),
                'end': str(now)
            })
            status_changed = True

        return json.dumps(history[-500:]), status_changed

    def mark_node(self, nodeid):
        with self.client.start_session() as session: 
            exists = self._nodes.find_one({"nodeid": nodeid}, session=session)

            if not exists:
                return (1, 'node with given id does not exist')

            now = time_now().replace(microsecond=0)
            history_json, status_changed = self._append_heartbeat_history(
                exists, now)
            updates = {
                'last_active': now,
                'availability_history_json': history_json
            }
            if status_changed:
                updates['last_status_change'] = now
            
            self._nodes.update_one({
                'nodeid': nodeid
            }, {
                '$set': updates
            },
            upsert=False, session=session)
        
        return (0, "updated node successfully")

    def get_nodes(self, nodeid=None, location=None, name=None, 
                        provider=None, active=True, activeThres=600,
                        owner=None):
        """get nodes"""
        query={}
        
        if nodeid:
            query["nodeid"] = nodeid 
        
        if location:
            query["location"] = location 
        
        if name:
            query["name"] = name 
        
        if provider:
            query["provider"] = provider 

        if owner:
            query["owner"] = owner
        
        if active:
            thres = time_now() - datetime.timedelta(seconds=activeThres)
            query["last_active"] = {}
            query["last_active"]["$gte"] = thres
        
        with self.client.start_session() as session:
            nodes = []
            res = self._nodes.find(query, session=session)

            for node in res:
                jobs_str = ''
                exists, jobs = self.get_jobs_by_nodeid(node['nodeid'])
                if exists:
                    for job in jobs:
                        msg_str = '<jobid=%s userid=%s type=%s start=%s end=%s length=%s schedule=%s overhead=%s server=%s trigger=%s>'
                        if job.type.lower() == 'cron':
                            msg = msg_str % (job.jobid, job.userid, job.type, str(job.start_date), str(job.end_date),
                                    str(job.length_secs), job.get_cron_string(), str(job.overhead), str(job.server), str(job.trigger))
                        
                        elif job.type.lower() == 'atq':
                            msg = msg_str % (job.jobid, job.userid, job.type, str(job.start_date), str(job.end_date),
                                    str(job.length_secs), '', str(job.overhead), str(job.server), str(job.trigger))

                        jobs_str += msg        

                node.pop('_id')
                nodeobj = LeotestNode(**node)
                nodeobj.jobs = jobs_str
                nodes.append(nodeobj.document_proto_compatible())
            
            return nodes
    

    def update_node(self, nodeid, 
            name=None, 
            description=None, 
            last_active=None,
            coords=None,
            location=None,
            provider=None,
            public_ip=None,
            owner=None,
            scheduling_enabled=None,
            registered_at=None,
            last_status_change=None,
            bandwidth_limits_json=None,
            availability_history_json=None):
        
        """update node"""
        updates={}
        
        if name:
            updates["name"] = name 

        if description:
            updates["description"] = description 

        if last_active:
            updates["last_active"] = last_active

        if coords:
            updates["coords"] = coords 

        if location:
            updates["location"] = location 
        
        if provider:
            updates["provider"] = provider 
        
        if public_ip:
            updates["public_ip"] = public_ip

        if owner is not None:
            updates["owner"] = owner

        if scheduling_enabled is not None:
            updates["scheduling_enabled"] = scheduling_enabled
            updates["last_status_change"] = time_now().replace(microsecond=0)

        if registered_at:
            updates["registered_at"] = datetimeParse(str(registered_at))

        if last_status_change:
            updates["last_status_change"] = datetimeParse(str(last_status_change))

        if bandwidth_limits_json is not None:
            updates["bandwidth_limits_json"] = bandwidth_limits_json

        if availability_history_json is not None:
            updates["availability_history_json"] = availability_history_json

        log.info('[update_node] nodeid=%s updates=%s' % (nodeid, str(updates)))
        with self.client.start_session() as session:
            self._nodes.update_one({
                'nodeid': nodeid
            }, 
            {
                '$set': updates
            },
            upsert=False, session=session)
            
        return (0, "updated node successfully")

    def get_scavenger_status(self, nodeid):
        """Return the scavenger_mode_active flag for a node, or None if not found."""
        query = {}
        if nodeid:
            query["nodeid"] = nodeid

        with self.client.start_session() as session:
            res = list(self._nodes.find(query, session=session))

        if res:
            node = res[0]
            # BUG FIX: nodes registered before the scavenger feature was added may not
            # have this field; use .get() with a False default instead of direct key access.
            scavenger_mode_active = node.get("scavenger_mode_active", False)
            log.info("[datastore] get_scavenger_status nodeid=%s scavenger_mode_active=%s",
                     nodeid, scavenger_mode_active)
            return scavenger_mode_active
        else:
            log.warning("[datastore] get_scavenger_status nodeid=%s not found in nodes collection",
                        nodeid)
            return None

    def set_scavenger_status(self, nodeid, scavenger_mode_status):
        """update a jobs start_time given jobid"""
        with self.client.start_session() as session:
            self._nodes.update_one({
                'nodeid': nodeid
            }, 
            {
                '$set': {'scavenger_mode_active': scavenger_mode_status}
            },
            upsert=False, session=session)
        
        return (0, "updated scavenger_mode_status successfully")
    
    def schedule_task(self, task: LeotestTask):
        """Persist a new task in MongoDB."""

        document = task.document()
        taskid = document['taskid']
        log.info("[datastore] schedule_task taskid=%s runid=%s jobid=%s nodeid=%s type=%s ttl_secs=%s",
                 taskid, document.get('runid'), document.get('jobid'),
                 document.get('nodeid'), document.get('task_type'), document.get('ttl_secs'))
        with self.client.start_session() as session:
            # NOTE: query uses "id" historically but the document stores "taskid"
            exists = self._tasks.find_one({"taskid": taskid}, session=session)
            if exists:
                log.warning("[datastore] schedule_task DUPLICATE taskid=%s already exists", taskid)
                return (1, 'task with the given id exists')
            self._tasks.insert_one(document, session=session)

        log.info("[datastore] schedule_task inserted taskid=%s", taskid)
        return (0, "task successfully scheduled")


    def get_tasks(self, taskid=None, runid=None, jobid=None, nodeid=None):
        """get tasks"""

        query={}
        
        if taskid:
            query["taskid"] = taskid 

        if runid:
            query["runid"] = runid

        if jobid:
            query["jobid"] = jobid

        if nodeid:
            query["nodeid"] = nodeid 
        
        with self.client.start_session() as session:
            tasks = []
            res = self._tasks.find(query, session=session)

            for task in res:        
                task.pop('_id')
                status = task['status']
                task.pop('status')
                task.pop('expire_at')
                taskobj = LeotestTask(**task)
                taskobj.set_status(status) 
                tasks.append(taskobj.document_proto_compatible())
            
            return tasks

    def update_task(self, taskid, status):
        """update task"""

        with self.client.start_session() as session: 
            exists = self._tasks.find_one({"taskid": taskid}, session=session)

            if not exists:
                return (1, 'task with given id does not exist')
            
            self._tasks.update_one({
                'taskid': taskid
            }, {
                '$set': {'status': status}
            },
            upsert=False, session=session)
        
        return (0, "updated task successfully")

        

# user = LeotestUser(id='test-user', 
#                     name='test user', 
#                     role=LeotestUserRoles.ADMIN,
#                     team='Project Leopard - MSRI')

# userNew = LeotestUser(id='test-user', 
#                     name='test user', 
#                     role=LeotestUserRoles.NODE,
#                     team='Project Leopard - MSRI')


# job_params = {
#     "mode": "docker",
#     "deploy": "repository=hello-world;tag=latest",
#     "execute": "image=hello-world",
#     "finish": ""
# }

# job = LeotestJobCron(jobid='test-job', 
#                     nodeid="test-node", 
#                     job_params=job_params)

# jobNew = LeotestJobCron(jobid='test-job', 
#                     nodeid="test-node", 
#                     job_params=job_params,
#                     minute='*/5')

# db = LeotestDatastoreMongo()



# print('allocating job')
# db.add_job(job)

# print('fetching jobs on test-node')
# print(db.get_jobs_by_nodeid('test-node'))


# print('get job by jobid')
# print(db.get_job_by_id('test-job'))

# print('modifying user')
# db.modify_job(jobNew)

# print('fetching jobs on test-node')
# print(db.get_jobs_by_nodeid('test-node'))

# print('deleting job')
# db.delete_job(jobNew)

# print('fetching jobs on test-node')
# print(db.get_jobs_by_nodeid('test-node'))

# print('inserting user')
# db.add_user(user)
# print('getting user')
# print(db.get_user('test-user').document())
# print('modifying user')
# db.modify_user(userNew)
# print('getting user')
# print(db.get_user('test-user').document())
# print('deleteing user')
# db.delete_user(userNew)
# print('getting user')
# print(db.get_user('test-user').document())
