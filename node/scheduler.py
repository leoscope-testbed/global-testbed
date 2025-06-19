import json
import os
import time
import redis 
import docker
import logging 
import threading
from multiprocessing import Process
from datetime import datetime, timedelta
from atd import atd
from subprocess import Popen, PIPE
from crontab import CronTab
from common.utils import time_now, get_public_ip, get_weather_mon_info
from common.client import LeotestClient
from common.job import LeotestJobCron, LeotestJobAtq, LeotestTask
from google.protobuf.json_format import MessageToDict
from pymemcache.client.base import Client as memcache_client

import common.leotest_pb2_grpc as pb2_grpc 
import common.leotest_pb2 as pb2

from node.trigger import LeotestTriggerMode, LeotestDockerNetworkMonitor,\
            LeotestGrpcMonitor, LeotestSatelliteMonitor, LeotestWeatherMonitor

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def kill_all_jobs(client, nodeid, sessionstore, resched_buffer=1800):
    """imple."""
    
    log.info("kill_all_jobs called.")
    docker_client = docker.from_env()

    filters = {
        "label": ["leotest=true", "overhead=true"], 
        "status": "running" 
    }
    try:
        container_list = docker_client.containers.list(filters=filters)

        for container in container_list:
            # update run status 
            labels = container.labels 
            runid = labels["runid"]
            jobid = labels["jobid"]
            job_type = labels["type"]
            end_date = labels["end_date"]
            client_params = {
                'runid': runid,
                'jobid': jobid,
                'nodeid': nodeid,
                'start_time': labels["start_time"]
            }
            log.info(client_params)
            res = sessionstore.get(key="%s_executor" % runid)
            log.info('executor session for runid=%s --> %s' % (runid, str(res)))

            container.stop()
            container.remove()

            # TODO: Handle a race condition with the executor. As soon as the 
            # the container exits, the executor starts finishing up the job (artifact upload, clean-up).
            # So there is a race condition between the reschedule status update by the scheduler (below) 
            # and the job status update that the executor will initiate. 
            tries = 100
            while sessionstore.get(key="%s_executor" % runid) != None and tries > 0:
                time.sleep(1)
                log.info('waiting for executor to exit (runid=%s) tries=%d' % (runid, tries))
                tries -= 1

            if job_type == "cron":
                log.info('aborting run (scavenger mode is active): runid=%s jobid=%s type=%s' 
                            % (client_params['runid'], client_params['jobid'], job_type))
                client.update_run(status='ABORTED', 
                        status_message='Job aborted: scavenger mode is active.',
                        **client_params)
                
            elif job_type == "atq":
                log.info('rescheduling job (scavenger mode is active): runid=%s jobid=%s type=%s' 
                            % (client_params['runid'], client_params['jobid'], job_type))
                

                starttime = time_now() + timedelta(seconds=int(resched_buffer))
                endtime = end_date
                ret = client.reschedule_job_nearest(
                                            jobid, str(starttime), str(endtime))

                # message message_reschedule_job_response {
                #     bool rescheduled = 1; 
                #     string message = 2; 
                # }

                terminate = {}
                msg = MessageToDict(ret)
                if 'rescheduled' in msg and msg['rescheduled']:
                    log.info('atq job rescheduled (scavenger mode is active): ' 
                              'runid=%s jobid=%s type=%s message=%s' 
                            % (client_params['runid'], client_params['jobid'], job_type, msg['message']))
                    
                    terminate['status'] = 'RESCHEDULED'
                    terminate['reason'] = 'job run rescheduled (scavenger mode is active); reason: %s' % msg['message']
                
                else:
                    log.info('atq job reschedule failed (scavenger mode is active): ' 
                              'runid=%s jobid=%s type=%s message=%s' 
                            % (client_params['runid'], client_params['jobid'], job_type, msg['message']))
                    
                    terminate['status'] = 'RESCHEDULE_FAILED'
                    terminate['reason'] = 'job run reschedule failed (scavenger mode is active); reason: %s' % msg['message']
                

                client.update_run(status=terminate['status'], 
                                status_message=terminate['reason'],
                                **client_params)
            
            else:
                log.info('job type not recognized: runid=%s jobid=%s type=%s'
                         % (client_params['runid'], client_params['jobid']), job_type)
            

    except Exception as e:
        log.info(e)

def kill_task_docker(runid, jobid):
    """kill a docker container given name"""
    log.info("kill_task_docker runid=%s jobid=%s" % (runid, jobid))
    client = docker.from_env()
    label = ["runid=%s"%runid, "jobid=%s" % jobid, "server=true"] 
    filters = {
        "label": label, 
        "status": "running" 
    }

    try:
        container_list = client.containers.list(filters=filters)
        log.info('kill_task_docker runid=%s jobid=%s container_list %s' 
                            % (runid, jobid, str(container_list)))
        container = container_list[0]
        log.info('kill_task_docker runid=%s jobid=%s stopping container' 
                            % (runid, jobid))
        container.stop()
        # log.info('removing container')
        # container.remove()
    except:
        log.info('kill_task_docker runid=%s jobid=%s no existing instance of measurement container found'
                    % (runid, jobid))

class LeotestTaskScheduler:
    def __init__(self, executor_path):
        self.executor_path = executor_path
    
    def _add_task(self, task):
        """add a task to task queue"""

    def add_task(self, task):
        self._add_task(task)

    def _sync_tasks(self, remote_task_list):
        """sync the tasks with the remote task list"""
    
    def sync_tasks(self, remote_task_list):
        self._sync_tasks(remote_task_list)

    def _remove_task(self, task):
        """remove task from task queue"""

    def remove_task(self, task):
        self._remove_task(task)

"""
Using python-atd:

from atd import atd
import datetime

print('Create some jobs...')
job1 = atd.at("echo lol >> /tmp/lolol", datetime.datetime.now() + 
    datetime.timedelta(minutes = 2))

jobid = job1.command.split('#')[1].strip()

# find jobs with a specific jobid: 
atq = atd.AtQueue()
local_jobs = {}

for job in atq.jobs:
    jobid = job.command.split('#')[1].strip()
    
    if jobid not in the remote_job_list:
        atd.atrm(job)
    else:
        local_jobs[jobid] = job

for jobid, job in remote_job_list:
    if jobid not in local_jobs:
        # schedule the job 
        atd.at("./job_command # %s" % jobid, datetime.datetime.now() + 
    datetime.timedelta(minutes = delta))

"""
# class LeotestTaskSchedulerAtq(LeotestTaskScheduler):
#     def __init__(self, executor_path, 
#                         nodeid,
#                         artifactdir="/artifacts/",
#                         grpc_hostname="localhost",
#                         grpc_port=50051,
#                         executor_config='/executor-config.yaml'):
#         super().__init__(executor_path=executor_path)

#         self.artifactdir = artifactdir
#         self.grpc_hostname = grpc_hostname
#         self.grpc_port = grpc_port
#         self.executor_config = executor_config
#         self.nodeid = nodeid

#     def _add_task(self, leotest_task):
#         cmd = self.executor_path        
        
#         cmd += " --taskid=%s" % leotest_task.get_taskid()
#         cmd += " --runid=%s" % leotest_task.get_runid()
#         cmd += " --jobid=%s" % leotest_task.get_jobid() 
#         cmd += " --nodeid=%s" % leotest_task.get_nodeid()
#         cmd += " --ttl-secs=%s" % leotest_task.get_ttl_secs()
#         cmd += " --server"
#         cmd += ' --workdir="%s"' % self.artifactdir
#         cmd += " --grpc-hostname=%s" % self.grpc_hostname
#         cmd += " --grpc-port=%s" % self.grpc_port
#         cmd += " --executor-config=%s" % self.executor_config
#         cmd += " --mode=docker"

#         log.info('task command: %s' % cmd)

#         logfile = os.path.join(self.artifactdir, "executor_stdout.log")
#         cmd += " >> %s" % logfile

#         # schedule the job using at
#         Popen(["at", "now"], stdin=PIPE, stdout=PIPE).communicate(
#             input=cmd.encode()
#         )
class LeotestTaskSchedulerPopen(LeotestTaskScheduler):
    """Inherited class for scheduling the tasks locally at a node.
    The jobs scheduled on the testbed are converte to local tasks at the nodes and the tasks are executed using the executor module.

    :param LeotestTaskScheduler: Base class that defines abstract methods to schedule tasks on the node.
    :type LeotestTaskScheduler: class LeotestTaskScheduler
    """    
    def __init__(self, executor_path,
                        module_name,
                        workdir, 
                        nodeid,
                        artifactdir="/artifacts/",
                        grpc_hostname="localhost",
                        grpc_port=50051,
                        executor_config='/executor-config.yaml',
                        access_token=''):
        super().__init__(executor_path=executor_path)

        self.executor_path = executor_path
        self.workdir = workdir 
        self.module_name = module_name
        self.artifactdir = artifactdir
        self.grpc_hostname = grpc_hostname
        self.grpc_port = grpc_port
        self.executor_config = executor_config
        self.nodeid = nodeid
        self.access_token = access_token

    def _add_task(self, leotest_task):
        cmd = [self.executor_path]        
        cmd.append("-m")
        cmd.append(self.module_name)
        cmd.append("--taskid=%s" % leotest_task.get_taskid())
        cmd.append("--runid=%s" % leotest_task.get_runid())
        cmd.append("--jobid=%s" % leotest_task.get_jobid()) 
        cmd.append("--nodeid=%s" % leotest_task.get_nodeid())
        cmd.append("--access-token=%s" % self.access_token)
        cmd.append("--ttl-secs=%s" % leotest_task.get_ttl_secs())
        cmd.append("--server")
        cmd.append("--workdir=%s" % self.artifactdir)
        cmd.append("--grpc-hostname=%s" % self.grpc_hostname)
        cmd.append("--grpc-port=%s" % self.grpc_port)
        cmd.append("--executor-config=%s" % self.executor_config)
        cmd.append("--mode=docker")

        
        log.info('task command: %s' % str(cmd))

        # logfile = os.path.join(self.artifactdir, "executor_stdout.log")
        # cmd += " >> %s" % logfile

        # schedule the job using Popen
        # Popen(["at", "now"], stdin=PIPE, stdout=PIPE).communicate(
            # input=cmd.encode()
        # )
        log.info('-------------------------TASK COMMAND-----------------------------------------')
        log.info(cmd)
        log.info('-------------------------TASK COMMAND:END-----------------------------------------')
        Popen(cmd, cwd=self.workdir, preexec_fn=os.setsid)


class LeotestJobScheduler:
    """Base class to handle jobs sent by the orchestrator at the nodes.
    """    
    def __init__(self, executor_path):
        self.executor_path = executor_path
    
    def _add_job(self, job):
        """add a job to crontab"""

    def add_job(self, job):
        self._add_job(job)
    
    def _remove_job(self, job):
        """remove job from crontab"""

    def remove_job(self, job):
        self._remove_job(job)
    
    def _sync_jobs(self, remote_job_list):
        """syncronize local job schedule with remote schedule"""

    def sync_jobs(self, remote_job_list):
        self._sync_jobs(remote_job_list)


    def _get_job_list(self):
        """return a list of jobs locally scheduled"""

    def get_job_list(self):
        jobs = self._get_job_list()
        return jobs

class LeotestJobSchedulerAtq(LeotestJobScheduler):
    """Inherited class responsible for handling ATQ jobs sent by the orchestrator at the nodes.

    :param LeotestJobScheduler: Base class that defines the various abstract methods.
    :type LeotestJobScheduler: class LeotestJobScheduler
    """    
    def __init__(self, executor_path, 
                        nodeid,
                        artifactdir="/artifacts/",
                        grpc_hostname="localhost",
                        grpc_port=50051,
                        executor_config='/executor-config.yaml',
                        access_token=''):
        super().__init__(executor_path=executor_path)

        self.artifactdir = artifactdir
        self.grpc_hostname = grpc_hostname
        self.grpc_port = grpc_port
        self.executor_config = executor_config
        self.nodeid = nodeid
        self.access_token = access_token
    
    # methods to handle atq interactions
    def get_jobid_from_command(self, cmd):
        return cmd.decode('utf-8').split('#')[1].strip()

    def get_job_with_id(self, jobid):
        job = None 
        job_found = False
        atq = atd.AtQueue()
        for _job in atq.jobs:
            if jobid == self.get_jobid_from_command(_job.command):
                print('job found: %s' % str(_job))
                job_found = True 
                job = _job
                break 

        # find jobs with a specific jobid: 
        return job_found, job 

    def get_job_with_id(self, jobid):
        job = None 
        job_found = False
        atq = atd.AtQueue()
        for _job in atq.jobs:
            if jobid == self.get_jobid_from_command(_job.command):
                print('job found: %s' % str(_job))
                job_found = True 
                job = _job
                break 

        # find jobs with a specific jobid: 
        return job_found, job 

    def list_all_jobs(self):
        jobs = []
        atq = atd.AtQueue()
        for _job in atq.jobs:
            jobid = self.get_jobid_from_command(_job.command)
            jobs.append(jobid)
        
        return jobs 

    def jobid_in_remote_list(self, jobid, remote_job_list):
        for job in remote_job_list:
            if jobid == job['jobid']:
                return True 
        
        return False

    def _add_job(self, leotest_job):
        """ This methods schedules the job sent by orchestrator as local tasks on the node.

        :param leotest_job: object of class LeotestJob
        :type leotest_job: object LeotestJob
        """        
        cmd = self.executor_path
        for key, value in leotest_job.get_job_params().items():
            cmd += " --%s='%s'" % (key, value)
        
        # append jobid 
        cmd += " --jobid=%s" % leotest_job.get_jobid() 
        cmd += " --start-date=%s" % leotest_job.start_date 
        cmd += " --end-date=%s" % leotest_job.end_date
        cmd += " --resched-buffer=%d" % 1800 # TODO: Make this configurable 
        cmd += " --length-secs=%s" % leotest_job.get_length_secs()
        cmd += " --nodeid=%s" % self.nodeid
        cmd += " --userid=%s" % leotest_job.userid
        cmd += " --access-token=%s" % self.access_token
        cmd += ' --workdir="%s"' % self.artifactdir
        cmd += " --grpc-hostname=%s" % self.grpc_hostname
        cmd += " --grpc-port=%s" % self.grpc_port
        cmd += " --executor-config=%s" % self.executor_config
        cmd += " --mode=docker"
        cmd += " --type=%s" % leotest_job.type 
        
        if leotest_job.overhead:
            cmd += " --overhead"
        else:
            cmd += " --no-overhead"

        if leotest_job.server:
            cmd += " --setup-server"
            cmd += " --server-node=%s" % leotest_job.server

        logfile = os.path.join(self.artifactdir, "executor_stdout.log")
        cmd += " >> %s" % logfile

        # get current time 
        if time_now() >= leotest_job.get_start_time_obj():
            # schedule now 
            log.info('not scheduling atq job now as time_now >="%s";' 
                    'cmd="%s # %s"' 
                    % (leotest_job.start_date, cmd, leotest_job.get_jobid()))
            
            # atd.at("%s # %s" % (cmd, leotest_job.get_jobid()), 
              #                                 time_now() + timedelta(seconds=10))
        else:
            log.info('scheduling atq job at time="%s";' 
                    'cmd="%s # %s"' % (leotest_job.start_date, 
                                        cmd, leotest_job.get_jobid()))
            atd.at("%s # %s" % (cmd, leotest_job.get_jobid()), 
                                              leotest_job.get_start_time_obj())

    def _sync_jobs(self, remote_job_list):
        """removes the existing jobs and reschedules the jobs obtained from the orchestrator.

        :param remote_job_list: List of jobs schedules sent by orchestrator.
        :type remote_job_list: list of objects of LeotestJob class.
        """        
        log.info('clearing existing atq jobs')
        # clear all jobs 
        atq = atd.AtQueue()

        try:
            for job in atq.jobs:
                atd.atrm(job)
        except Exception as e:
            log.info(e)

        local_jobs = {}
        atq = atd.AtQueue()
        for job in atq.jobs:
            jobid = self.get_jobid_from_command(job.command)
            
            if not self.jobid_in_remote_list(jobid, remote_job_list):
                atd.atrm(job)
            else:
                local_jobs[jobid] = job

        # print(local_jobs)
        log.info('syncing remote jobs: %s' % str(remote_job_list))
        for job in remote_job_list:
            jobid = job.jobid
            if jobid not in local_jobs:
                print('schedule %s' % jobid)
                # schedule the job 
                self.add_job(job)

class LeotestJobSchedulerCron(LeotestJobScheduler):
    """Inherited class responsible for handling CRON jobs sent by the orchestrator at the nodes.

    :param LeotestJobScheduler: Base class that defines the various abstract methods.
    :type LeotestJobScheduler: class LeotestJobScheduler
    """    
    def __init__(self, executor_path, 
                        nodeid,
                        artifactdir="/artifacts/",
                        grpc_hostname="localhost",
                        grpc_port=50051,
                        executor_config='/executor-config.yaml',
                        access_token=''):
        super().__init__(executor_path=executor_path)
        self.cron = CronTab(user=True)
        self.artifactdir = artifactdir
        self.grpc_hostname = grpc_hostname
        self.grpc_port = grpc_port
        self.executor_config = executor_config
        self.nodeid = nodeid
        self.access_token = access_token

    def _add_job(self, leotest_job):
        cmd = self.executor_path
        for key, value in leotest_job.get_job_params().items():
            cmd += " --%s='%s'" % (key, value)
        
        # append jobid 
        cmd += " --jobid=%s" % leotest_job.get_jobid() 
        cmd += " --start-date=%s" % leotest_job.start_date 
        cmd += " --end-date=%s" % leotest_job.end_date
        cmd += " --resched-buffer=%d" % 1800 # TODO: Make this configurable 
        cmd += " --length-secs=%s" % leotest_job.get_length_secs()
        cmd += " --nodeid=%s" % self.nodeid
        cmd += " --userid=%s" % leotest_job.userid
        cmd += " --access-token=%s" % self.access_token
        cmd += ' --workdir="%s"' % self.artifactdir
        cmd += " --grpc-hostname=%s" % self.grpc_hostname
        cmd += " --grpc-port=%s" % self.grpc_port
        cmd += " --executor-config=%s" % self.executor_config
        cmd += " --mode=docker"
        cmd += " --type=%s" % leotest_job.type 

        if leotest_job.overhead:
            cmd += " --overhead"
        else:
            cmd += " --no-overhead"

        if leotest_job.server:
            cmd += " --setup-server"
            cmd += " --server-node=%s" % leotest_job.server

        logfile = os.path.join(self.artifactdir, "executor_stdout.log")
        cmd += " >> %s" % logfile

        job  = self.cron.new(command=cmd, 
                            comment=leotest_job.get_jobid())
        job.setall(leotest_job.get_cron_string())
        self.cron.write() 
    
    def _remove_job(self, leotest_job):
        self.cron.remove_all(comment=leotest_job.get_jobid())
        self.cron.write()

    def _sync_jobs(self, remote_leotest_jobs):
        # clear all jobs and add them again 
        # this ensures that any modifications to cron schedules are replicated
        self.cron.remove_all()
        for leotest_job in remote_leotest_jobs:
            self.add_job(leotest_job)
        # in case there are no jobs, ensure we perform a write() to clear the crontab
        self.cron.write()
    
    def get_params_from_cmd(self, cmd):
        params = {}
        tokens = cmd.split(" ")
        for i in range(1, len(tokens)):
            arg = tokens[i]
            arg_split = arg.split('=')
            key = arg_split[0][2:]
            val = arg_split[1]
            params[key] = val
        return params

    def _get_job_list(self):
        leotest_job_list = []
        for job in self.cron:
            jobid = job.comment 
            job_params = self.get_params_from_cmd(job.command) 
            # TODO: parse command to get job parameters 

            leotest_job = LeotestJobCron(jobid = jobid,
                                    job_params=job_params,  
                                    minute=str(job.minute),
                                    hour=str(job.hour),
                                    day_of_month=str(job.dom),
                                    month=str(job.month),
                                    day_of_week=str(job.dow))

            leotest_job_list.append(leotest_job)
        
        return leotest_job_list


def _scheduler_execute(nodeid, client, cron_scheduler, atq_scheduler, task_scheduler, 
                           sessionstore, trigger_module, log):
    
    client.init_grpc_client() # reset client state as socket does not exist on new process
    log.info('sending heartbeat')
    client.send_heartbeat(nodeid)
    client.update_node(nodeid=nodeid, public_ip=get_public_ip())

    log.info('fetching jobs from orchestrator')
    res = MessageToDict(client.get_jobs_by_nodeid(nodeid))
    # {'exists': True, 'jobs': [{'id': 'test-id-1', 'nodeid': 'test-node', 'params': {'mode': 'docker', 'deploy': 'random deploy', 'execute': 'random execute', 'finish': 'random finish'}, 'schedule': '*/6 * * * *'}, {'id': 'test-id-2', 'nodeid': 'test-node', 'params': {'mode': 'docker', 'deploy': 'random deploy', 'execute': 'random execute', 'finish': 'random finish'}, 'schedule': '*/6 * * 2-10 *'}]}
    print(res)

    remote_job_list = {'cron': [], 'atq': []}

    if 'jobs' in res:
        logmsg = 'syncing jobs: '
        for job in res['jobs']:
            # Saving experiment config in file:
            log.info(f"reading job config for job id {job['id']}!")
            retrieve_job = client.get_job_by_id(job["id"])
            data = retrieve_job.config
            lines = data.split('\n')
            result_dict = {}
            stack = [result_dict]

            for line in lines:
                if not line.strip().startswith('#') and line.strip() != '':
                    indentation = len(line) - len(line.lstrip())
                    while len(stack) > indentation + 1:
                        stack.pop()
                    key, value = map(str.strip, line.split(':', 1))
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        pass
                    stack[-1][key] = value
                    if isinstance(value, dict):
                        stack.append(value)

            log.info(f"finished reading job config for job id {job['id']}!")
            log.info(f"experiment config result for job id {job['id']} is: {result_dict}")

            file_name = f"/leotest/experiment_configs/experiment_config_{job['id']}.json"

            # Save data to the file inside the container
            with open(file_name, "w") as file:
                json.dump(result_dict, file)

            if nodeid != job['nodeid']:
                continue
            server = job['server'] if 'server' in job else None
            trigger = job['trigger'] if 'trigger' in job else None         
            job_type = job['type'].lower() if 'type' in job else 'cron'
            if not 'overhead' in job:
                job['overhead'] = False
            logmsg += '<id=%s,type=%s>' % (job['id'], job_type)

            if job_type == 'cron':
                leo_job = LeotestJobCron(jobid=job['id'],
                                        nodeid=job['nodeid'],
                                        userid=job['userid'],
                                        start_date=job['startDate'],
                                        end_date=job['endDate'],
                                        server=server,
                                        trigger=trigger,
                                        length_secs=job['lengthSecs'],
                                        job_params=job['params'],
                                        overhead=job['overhead'])
                leo_job.set_schedule_cron(job['schedule'])
                remote_job_list['cron'].append(leo_job)
            
            elif job_type == 'atq':
                leo_job = LeotestJobAtq(jobid=job['id'],
                    nodeid=job['nodeid'],
                    userid=job['userid'],
                    start_date=job['startDate'],
                    end_date=job['endDate'],
                    server=server,
                    trigger=trigger,
                    length_secs=job['lengthSecs'],
                    job_params=job['params'],
                    overhead=job['overhead'])
                
                remote_job_list['atq'].append(leo_job)
        
        log.info(logmsg)

    cron_scheduler.sync_jobs(remote_job_list['cron'])
    atq_scheduler.sync_jobs(remote_job_list['atq'])
    for key in remote_job_list:
        trigger_module.sync_triggers(remote_job_list[key])

    log.info('fetching tasks from orchestrator')
    res = MessageToDict(client.get_tasks(nodeid=nodeid))
    print(res)

    if 'tasks' in res:
        for task in res['tasks']:
            log.info(task)
            task_type = task['type']
            leo_task = LeotestTask(taskid=task['taskid'],
                                    runid=task['runid'],
                                    jobid=task['jobid'],
                                    nodeid=task['nodeid'],
                                    task_type=task['type'],
                                    ttl_secs=task['ttlSecs'])
            
            if 'status' in task and task['status'] != 'TASK_COMPLETE':
                # task_type_str = pb2.task_type.Name(task_type)
                exists = sessionstore.get(key=task['taskid'], default=None)
                if not exists:
                    log.info('task %s does not exist. Adding to sessionstore'
                                % (task['taskid']))
                    sessionstore.set(key=task['taskid'], 
                                        value='1', 
                                        # expire=int(task['ttlSecs']) + 30
                                        expire=24*60*60) # 24 hours 
                    
                    if task_type == 'SERVER_START':
                        # add to task scheduler 
                        task_scheduler.add_task(leo_task)

                    elif task_type == "SERVER_STOP":
                        # get the container name and kill it 
                        kill_task_docker(leo_task.get_runid(), leo_task.get_jobid())
                        client.update_task(taskid=task['taskid'], status='TASK_COMPLETE')

                    else:
                        log.info('no such task %s' % task_type) 

                else:
                    log.info('task %s exists, not adding.' % (task['taskid']))
    
    # check for scavenger status, if there are jobs running, bring them down 
    msg = MessageToDict(client.get_scavenger_status(nodeid))
    log.info(msg)
    if 'scavengerModeActive' in msg and msg['scavengerModeActive']==True:
        log.info('scavenger mode is active; killing all job containers.')
        # bring down all containers belonging to LEOScope 
        # we want to execute this async. to avoid schduler loop stalls.
        thread = threading.Thread(target=kill_all_jobs, 
                                    args=[client, nodeid, sessionstore])
        thread.start()
        # kill_all_jobs(client, nodeid, sessionstore)

def scheduler_loop(nodeid, 
                    grpc_hostname='localhost', 
                    grpc_port=50051, 
                    interval=5,
                    workdir="/home/leotest/",
                    artifactdir="/artifacts/",
                    executor_config="/executor-config.yaml",
                    access_token=""):


    log.info("entered scheduler loop")
    sessionstore = memcache_client('memcached:11211')

    # client = LeotestClient(
    #     grpc_hostname=grpc_hostname, 
    #     grpc_port=grpc_port,
    #     userid=nodeid, access_token=access_token)
    
    exec_hook = "cd %s && /usr/local/bin/python -m node.executor" % workdir
    cron_scheduler = LeotestJobSchedulerCron(executor_path=exec_hook,
                                        nodeid=nodeid,
                                        artifactdir=artifactdir,
                                        grpc_hostname=grpc_hostname, 
                                        grpc_port=grpc_port,
                                        executor_config=executor_config,
                                        access_token=access_token)


    atq_scheduler = LeotestJobSchedulerAtq(executor_path=exec_hook,
                                        nodeid=nodeid,
                                        artifactdir=artifactdir,
                                        grpc_hostname=grpc_hostname, 
                                        grpc_port=grpc_port,
                                        executor_config=executor_config,
                                        access_token=access_token)

    # task_scheduler = LeotestTaskSchedulerAtq(executor_path=exec_hook,
    #                                     nodeid=nodeid,
    #                                     artifactdir=artifactdir,
    #                                     grpc_hostname=grpc_hostname, 
    #                                     grpc_port=grpc_port,
    #                                     executor_config=executor_config)

    task_scheduler = LeotestTaskSchedulerPopen(
                                executor_path="/usr/local/bin/python",
                                module_name="node.executor",
                                workdir=workdir,
                                nodeid=nodeid,
                                artifactdir=artifactdir,
                                grpc_hostname=grpc_hostname, 
                                grpc_port=grpc_port,
                                executor_config=executor_config,
                                access_token=access_token)

    """
    pull node information
    """
    client = LeotestClient(
        grpc_hostname=grpc_hostname, 
        grpc_port=grpc_port,
        userid=nodeid, access_token=access_token)
    
    res = client.get_config()

    log.info(MessageToDict(res))
    config = MessageToDict(res)['config']
    weather_api_key = config['weather']['apikey']

    res = client.get_nodes(nodeid=nodeid)
    msg = MessageToDict(res)
    log.info(msg)

    if 'nodes' in msg and len(msg['nodes']) >=1:
        coords = MessageToDict(res)['nodes'][0]['coords']
    else:
        log.error('node not registered with the orchestrator.')
        exit(1)

    log.info(MessageToDict(res))
    lat, lon = coords.split(',')


    trigger_module = LeotestTriggerMode()
    dockermon = LeotestDockerNetworkMonitor(trigger_module)
    log.info('init dockermon')
    # dockermon.run_async()

    i_api, i_lat, i_lon, i_ele = get_weather_mon_info()
    satmon = LeotestSatelliteMonitor(trigger_module, name=nodeid, lat=lat, lon=lon, ele=i_ele)
    log.info('init satmon')
    satmon.run_async()
    

    weathermon = LeotestWeatherMonitor(trigger_module, api=i_api, lat=i_lat, lon=i_lon, api_key=weather_api_key)
    log.info('init weathermon')
    weathermon.run_async()

    # check if terminal exists 
    log.info('check if terminal exists')
    r = redis.Redis(host='redis', port='6379', db=0, decode_responses=True)
    while True:
        dish_status = r.get('starlink-terminal-found')
        log.info('dish_status=%s' % dish_status)
        if not dish_status:
            time.sleep(1)
            log.info('waiting for terminal (dish) status...')
        else:
            if dish_status == 'True':
                log.info('getting starlink terminal id')
                utid = r.get('starlink-terminal-id')
                log.info('starlink terminal found; utid=%s; initiating gRPC monitor.' % utid)

                grpcmon = LeotestGrpcMonitor(trigger_module, utid, 
                            fields=['uplink_throughput_bps', 
                                    'downlink_throughput_bps', 
                                    'pop_ping_latency_ms',
                                    'direction_azimuth',
                                    'direction_elevation',
                                    'currently_obstructed',
                                    'fraction_obstructed'])
                grpcmon.run(run_async=True)
            else:
                log.info('no starlink terminal found; skipping init gRPC monitor.')
            
            break

    while True:
        # _scheduler_execute(nodeid, client, cron_scheduler, atq_scheduler, task_scheduler, 
        #                    sessionstore, trigger_module, log)
        args = (nodeid, client, cron_scheduler, atq_scheduler, task_scheduler, 
                           sessionstore, trigger_module, log, )
        p = Process(target=_scheduler_execute, args=args)
        p.start()
        p.join(60) # wait 60 seconds for each scheduler invocation
        if p.is_alive():
            p.terminate()

        time.sleep(interval)

# print('--- Test Job ---')
# job_params = {
#     "mode": "docker",
#     "deploy": "repository=hello-world;tag=latest",
#     "execute": "image=hello-world",
#     "finish": ""
# }

# job = LeotestJobCron(jobid='test_job', job_params=job_params)

# print(datetime.now())
# print(job.next_trigger_time())
# print(job.serialize())

# print('--- Test scheduler ---')
# scheduler = LeotestJobSchedulerCron(executor_path='./executor.py')

# jobs = scheduler.get_job_list()
# print('current jobs: ', jobs)

# print('adding job', job)
# scheduler.add_job(job)

# jobs = scheduler.get_job_list()
# print('current jobs: ', jobs)
# print(jobs[0].serialize())

# print('removing job')
# scheduler.remove_job(job)

# jobs = scheduler.get_job_list()
# print('current jobs: ', jobs)

