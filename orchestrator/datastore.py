'''
Contains code for interaction between the orchestrator and the Mongo database.
Database contains information about the users, nodes, job schedules, runs (status of the jobs, link to the artifacts), tasks and the testbed configs.
'''


from pymongo import ASCENDING, DESCENDING, DeleteOne, MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, DuplicateKeyError
from typing import List
import logging 

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
        
        self._tasks.create_index('expire_at', expireAfterSeconds=0)
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
            print(config)
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
            print("Getting config")
            config = self._config.find_one({"id": 0}, 
                                        session=session)
        return (0, config)

    def add_user(self, user: LeotestUser):
        """register user on leotest"""

        document = user.document()
        with self.client.start_session() as session: 
            exists = self._users.find_one({"id": document['id']}, 
                                                    session=session)
            if exists: 
                return (1, "user with given id already exists")
            else:
                self._users.insert_one(document, session=session)
                return (0, "user registered")

    def get_user(self, userid) -> LeotestUser:
        with self.client.start_session() as session:
            user = self._users.find_one({"id": userid}, 
                                        session=session)
            
            if user:
                return LeotestUser(id=user['id'], 
                                    name=user['name'], 
                                    role=user['role'], 
                                    team=user['team'],
                                    static_access_token=user['static_access_token'],
                                    access_token=user['access_token'])
            else:
                return None


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
        """add/update run on Leotest"""

        document = run.document()
        with self.client.start_session() as session: 
            runid = document['runid']
            self._runs.update_one({
                'runid': runid
            }, {
                '$set': document
            },
            upsert=True, session=session)
        
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

    def mark_node(self, nodeid):
        with self.client.start_session() as session: 
            exists = self._nodes.find_one({"nodeid": nodeid}, session=session)

            if not exists:
                return (1, 'node with given id does not exist')
            
            self._nodes.update_one({
                'nodeid': nodeid
            }, {
                '$set': {'last_active': time_now()}
            },
            upsert=False, session=session)
        
        return (0, "updated node successfully")

    def get_nodes(self, nodeid=None, location=None, name=None, 
                        provider=None, active=True, activeThres=600):
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
            public_ip=None):
        
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
        """get nodes"""
        query={}
        
        if nodeid:
            query["nodeid"] = nodeid 
        
        with self.client.start_session() as session:
            res = list(self._nodes.find(query, session=session))

            if res:
                node = res[0]
                scavenger_mode_active = node["scavenger_mode_active"]
                return scavenger_mode_active

            else:
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
        """schedule task"""
        
        document = task.document()
        with self.client.start_session() as session: 
            exists = self._tasks.find_one({"id": document['taskid']}, 
                                                    session=session)
            if exists: 
                return (1, 'task with the given id exists')
            else:
                self._tasks.insert_one(document, session=session)
        
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
