#!/usr/bin/python3
'''
Responsible for handling the experiment lifecycle, from setting up the nodes for deploying the experiments, to executing the experiments, 
uploading the experiment artifacts and finally cleaning the node and making it ready for the next experiment.
'''

import os
import sys
import uuid
import yaml
import json
import shutil
import docker 
import getpass
import logging
import argparse
import traceback
import threading
from time import sleep
from datetime import datetime, timedelta
from dateutil.parser import parse as datetimeParse

from google.protobuf.json_format import MessageToDict
from pymemcache.client.base import Client as memcache_client

from common.client import LeotestClient
from common.azure import download_file, upload_folder
from common.utils import route, time_now, TerminalGrpcDataCsv, make_archive, StorageDirectoryClient

import common.leotest_pb2_grpc as pb2_grpc
import common.leotest_pb2 as pb2

def check_container_running(client, container_name):
    return client.containers.get(container_name).status == 'running'

def check_task_status(taskid, client):
    res = MessageToDict(client.get_tasks(taskid=taskid))
    print(res)
    status = 0
    if 'tasks' in res:
        status = res['tasks'][0]['status']
    return status 

# take care of experiment life-cycle as well
class LeotestExecutor:
    """
    Class for maintaining the lifecycle of the experiments running on the nodes.
    The class includes methods for deploying, executing and finishing the experiments.
    """    
    def __init__(self, params):
        """Initializing the LeotestExecutor class.
        """        
        self.params = params
        if not self.params["server_mode"]:
            self.client_params = {
                'runid': self.params['runid'], 
                'jobid': self.params['jobid'], 
                'nodeid': self.params['nodeid'], 
                'userid': self.params['userid'],
                'start_time': self.params['start_time']
            }
        else:
            self.client_params = {
                'runid': self.params['runid_server'], 
                'jobid': self.params['jobid'], 
                'nodeid': self.params['nodeid'], 
                'userid': self.params['userid'],
                'start_time': self.params['start_time']
            }

        self._route_enabled = False
        if 'route' in params['executor']:
            self._route_enabled = True
            self._route_ip = params['executor']['route']['ip']
            self._route_gw = params['executor']['route']['gateway']
            self._route_dev = params['executor']['route']['dev']
        
        self.log = params['log']
        self.executor_log_stdout = params['executor_log_stdout']
        self.executor_log_stderr = params['executor_log_stderr']

        self.terminal_grpc_poll = TerminalGrpcDataCsv(api_path="/leotest/starlink-grpc-tools", 
                            logfile='%s/grpc.csv' % self.params['workdir'])
        
        self.log.info('starting terminal gRPC API poll')
        self.terminal_grpc_poll.run(_async=True)
    
    def _deploy_job(self):
        """prepare job environment before execution"""
    
    def deploy_job(self):
        self.log.info("starting job deployment")
        # self.params['client'].update_run(status='DEPLOYING', 
        #                                 status_message='Preparing environment for job execution.',
        #                                 **self.client_params)
        
        if (not self.params["server_mode"]) and self.params['setup_server']:
            self.params['client'].update_run(status='DEPLOYING', 
                                        status_message='Preparing environment for job execution: setting up server node.',
                                        **self.client_params)
            self.log.info('sending message to start server')
            taskid = uuid.uuid4().hex
            timeout = self.params['length_secs']
            self.params['client'].schedule_task(taskid=taskid, 
                                                runid=self.params['runid'], 
                                                jobid=self.params['jobid'], 
                                                nodeid=self.params['server_node'], 
                                                _type='SERVER_START', 
                                                ttl_secs=timeout)
            
            # wait for task to complete 
            stop_time = 3
            elapsed_time = 0
            while elapsed_time < timeout:
                status = check_task_status(taskid, self.params['client'])
                if status=='TASK_COMPLETE': # TODO: change this to explicit enum names 
                    self.log.info('server setup complete, proceeding.')
                    break
                
                self.log.info('waiting for server setup elapsed_time=%d timeout=%d' 
                                                    % (elapsed_time, timeout))
                sleep(stop_time)
                elapsed_time += stop_time

        # if routing is enabled, setup routing 
        if self._route_enabled:
            route("del", self._route_ip, self._route_gw, self._route_dev)
            self.log.info('Adding route dst=%s gw=%s dev=%s' % (
                self._route_ip, 
                self._route_gw,
                self._route_dev
            ))
            route("add", self._route_ip, self._route_gw, self._route_dev)

        self._deploy_job()

    def _execute_job(self):
        """execute job"""
    
    def execute_job(self):
        self.log.info("starting job execution")
        self.params['client'].update_run(status='EXECUTING', 
                                            status_message='Executing job.',
                                            **self.client_params)
        try:
            self._execute_job()
        except Exception as e:
            self.log.info(e)
            self.log.info(traceback.format_exc())
    
    def _finish_job(self):
        """perform post-execution tasks and cleanup"""
    
    def finish_job(self):
        self.log.info("performing job teardown")

        if not self.params["server_mode"]:
            if self.params['setup_server']:
                self.params['client'].update_run(status='FINISHING', 
                                            status_message='Performing post-execution tasks: stopping server',
                                            **self.client_params)

                self.log.info('sending message to stop server')
                taskid = uuid.uuid4().hex
                timeout = self.params['length_secs']
                self.params['client'].schedule_task(taskid=taskid, 
                                                    runid=self.params['runid'], 
                                                    jobid=self.params['jobid'], 
                                                    nodeid=self.params['server_node'], 
                                                    _type='SERVER_STOP', 
                                                    ttl_secs=timeout)

            self.params['client'].update_run(status='FINISHING', 
                                            status_message='Performing post-execution tasks: removing routes.',
                                            **self.client_params)
        # if routing is enabled, remove route 
        if self._route_enabled:
            self.log.info('Removing route dst=%s gw=%s dev=%s' % (
                self._route_ip, 
                self._route_gw,
                self._route_dev
            ))
            route("del", self._route_ip, self._route_gw, self._route_dev)

        self.log.info('stopping terminal gRPC API polling')
        self.terminal_grpc_poll.stop()
        archive_path = "%s.zip" % self.params['workdir']
        if self.params['server_mode']:
            archive_path_remote = "%s/%s_server.zip" % (self.params['remote_path'], self.params['runid'])
        else:
            archive_path_remote = "%s/%s.zip" % (self.params['remote_path'], self.params['runid'])
        self.log.info('archiving job data %s --> %s' % (self.params['workdir'], archive_path))
        make_archive(self.params['workdir'], "%s.zip" % self.params['workdir'])
        self.log.info('uploading job artifacts to blob storage (%s:%s.zip) --> %s.zip'
                            % (self.params['container'], 
                               self.params['remote_path'], 
                               self.params['workdir']))
        
        
        self.params['client'].update_run(status='FINISHING', 
                                        status_message='Performing post-execution tasks: uploading artifacts.',
                                        **self.client_params)

        # close executor logs before pushing the artifacts
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

        # reset logging handlers before closing log files 
        fileh = logging.StreamHandler(stream=sys.stdout)
        formatter = logging.Formatter("%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
        fileh.setFormatter(formatter)

        self.log = logging.getLogger()  # root logger
        for hdlr in self.log.handlers[:]:  # remove all old handlers
            self.log.removeHandler(hdlr)
        self.log.addHandler(fileh)      # set the new handler


        self.executor_log_stdout.close()
        self.executor_log_stderr.close()

        azclient = StorageDirectoryClient(self.params['connection_string'], self.params['container'])
        azclient.upload_file(archive_path, archive_path_remote)
        # upload_folder(self.params['connection_string'], 
        #             self.params['container'], 
        #             self.params['workdir'], 
        #             self.params['remote_path'], 
        #             overwrite=True)

        # get SAS token for the uploaded blob
        blob_url = azclient.get_sas_url(archive_path_remote)

        self.log.info('blob_url=%s' % blob_url)
        self.log.info('clearing artifacts from local storage')

        self.params['client'].update_run(
                                        blob_url=blob_url,
                                        status='FINISHING', 
                                        status_message='Performing post-execution tasks: Clearing artifacts from local storage.',
                                        **self.client_params)
        
        os.remove(archive_path)
        shutil.rmtree(self.params['workdir'])
        self._finish_job()
        # TODO: Implement artifact export 

        end_time = str(time_now())
        self.params['client'].update_run(
                                        end_time=end_time,
                                        blob_url=blob_url,
                                        status='COMPLETE', 
                                        status_message='Done executing the job.',
                                        **self.client_params)
    
    def run(self):
        # check if terminate is set
        if self.params['terminate']['flag']:
            self.params['client'].update_run(status=self.params['terminate']['status'], 
                                    status_message=self.params['terminate']['reason'],
                                    **self.client_params)
        
        else:
            self.deploy_job()
            self.execute_job()
            self.finish_job()

class LeotestExecutorDocker(LeotestExecutor):
    def __init__(self, params):
        super().__init__(params)
        self.client = docker.from_env()
        self.container_name = self.params["experiment"]["docker"]["execute"]["name"]
        self.container_name += "_" + self.params["jobid"] + "_" + self.params['runid']
        if self.params["server_mode"]:
            self.container_name += "_server"
        self.container = None

    def _deploy_job(self):
        """
        1. pull/build container image
        """
        # self.client.images.pull(**self.params['deploy'])
        # stop any existing instance of the container 
        image = self.params["experiment"]["docker"]["image"]
        self.log.info('pulling image %s' % (image))
        self.client.images.pull(image)
        
        try:
            # container_name = self.params["experiment"]["docker"]["execute"]["name"]
            container = self.client.containers.get(self.container_name)
            self.log.info('existing instance of measurement container found. Stopping...')
            container.stop()
            container.remove()
        except:
            self.log.info('no existing instance of measurement container found')

    def _execute_job(self):

        thread = threading.Thread(target=self._execute_job_loop, name=self.container_name)
        thread.start()
        if self.params["server_mode"]:
            self.log.info('running in server mode')
            ttl_secs = self.params['ttl_secs']
        else:
            self.log.info('running in client mode')
            ttl_secs = self.params['length_secs']

        # check if the container has started running 
        time_start = time_now()            
        delta = 0
        while thread.is_alive and delta <= ttl_secs:
            try:
                container = self.client.containers.get(self.container_name)
                if container.status != 'running':
                    delta = int((time_now() - time_start).total_seconds())
                    self.log.info('waiting for container to start running; %ss elapsed; ttl_secs (before job teardown)=%ss' % (delta, ttl_secs))
                    sleep(2)
                else:
                    if self.params["server_mode"]:
                        self.log.info('container started running; updating task status')
                        self.params['client'].update_task(taskid=self.params['taskid'], status='TASK_COMPLETE')
                    else:
                        self.log.info('container started running')

                    break
            except docker.errors.NotFound as e:
                self.log.info(e)
                delta = int((time_now() - time_start).total_seconds())
                self.log.info('container not found; retrying; %ss elapsed; ttl_secs (before job teardown)=%ss' % (delta, ttl_secs))
                sleep(2)
                continue
            except Exception as e:
                self.log.info(e)
                break
        
        # Substract the delta from ttl_secs. This ensures strict boundaries 
        # between closely spaced experiments. 
        ttl_secs = max(0, ttl_secs - delta)
        time_start = time_now()            
        delta = 0
        # kill the container after TTL
        while check_container_running(self.client, self.container_name) and delta <= ttl_secs:
            sleep(1)
            delta = int((time_now() - time_start).total_seconds())

            if delta%5==0:
                self.log.info('%ss elapsed. ttl_secs (before job teardown)=%s' % (delta, ttl_secs))

        if check_container_running(self.client, self.container_name):
            self.log.info('job run timeout exceeded; stopping container...')
            container = self.client.containers.get(self.container_name)
            container.stop()
        else:
            self.log.info('container exited.')
        
        # invalidate session if any 
        # NOTE: Commented this out as task sessions are set to expire in a few hours
        # so this is not needed. Explicitely invalidating sessions leads to spurious 
        # tasks being scheduled after the previous task is over 
        
        # if self.params['taskid']:
        #     self.log.info('invalidating session for taskid=%s' % self.params['taskid'])
        #     sessionstore = memcache_client('memcached:11211')
        #     exists = sessionstore.get(key=self.params['taskid'], default=None)
        #     if exists:
        #         sessionstore.delete(key=self.params['taskid'])
        
        thread.join()

    def _execute_job_loop(self):
        """
        2. launch container
        """
 
        self.log.info("launching measurement container")
        src_path = os.path.join(
            self.params["executor"]["docker"]["execute"]["volume"]["source"],
            self.params['expdir'])
        dst_path = self.params["executor"]["docker"]["execute"]["volume"]["dest"]

        network_mode = None
        network = None

        if 'network' in self.params["executor"]["docker"]["execute"]:
            network = self.params["executor"]["docker"]["execute"]["network"]
        else:
            network_mode = 'host'

        # ports = None
        # self.log.info("ports params are *** %s" % self.params["experiment"]["docker"]["execute"])
        # if network != 'host' and 'ports' in self.params["experiment"]["docker"]["execute"]:
        #         ports = self.params["experiment"]["docker"]["execute"]["ports"]

        image = self.params["experiment"]["docker"]["image"]
        
        environment = ["LEOTEST_SERVER=0"]
        if self.params["server_mode"]:
            environment = ["LEOTEST_SERVER=1"]
        
        environment.append("LEOTEST_JOBID=%s" % self.params["jobid"])
        environment.append("LEOTEST_NODEID=%s" % self.params["nodeid"])

        if self.params["server_ip"]:
            environment.append("LEOTEST_SERVERIP=%s" % self.params["server_ip"])
            environment.append("LEOTEST_SERVER_NODEID=%s" % self.params["server_node"])
        else:
            environment.append("LEOTEST_SERVERIP=None")
            environment.append("LEOTEST_SERVER_NODEID=None")

        # volume
        shared = {src_path: {'bind': dst_path, 'mode': 'rw'}}
        print(shared)
        # shared = {dst_path: {'bind': src_path, 'mode': 'rw'}}

        labels = {
            "runid": self.params["runid"],
            "jobid": self.params["jobid"],
            "userid": self.params["userid"],
            "start_time": self.params["start_time"],
            "start_date": self.params["start_date"],
            "end_date": self.params["end_date"],
            "type": self.params["job_type"],
            "overhead": "true" if self.params["overhead"] else "false",
            "server": "true" if self.params["server_mode"] else "false",
            "leotest": "true"
        }
        
        self.log.info('container network: %s' % network)
        # self.log.info('container ports: %s' % str(ports))
        self.log.info('--- log stream started for %s ---' % (self.container_name))

        try:
            output = self.client.containers.run(
                image=image,
                user=os.getuid(),
                detach=False,
                name=self.container_name,
                network_mode=network_mode,
                privileged=False,
                network=network,
                stdin_open=True,
                tty=False,
                mem_limit="512m",
                volumes=shared,
                # ports=ports,
                stdout=True,
                stderr=True,
                stream=True,
                environment=environment,
                labels=labels
            )
        except Exception as e:
            self.log.info(str(e))
            self.log.info('exception in container run. Getting logs...')
            # get name of the container 
            container = self.client.containers.get(self.container_name)
            output = container.logs(stdout=True, stderr=True, stream=True)

        try:
            while True:
                line = next(output).decode("utf-8").rstrip()
                self.log.info(line)
        except StopIteration:
            self.log.info('--- log stream ended for %s ---' % (self.container_name))  
    
    def _finish_job(self):
        """
        3. remove container 
        """
        try:
            container = self.client.containers.get(self.container_name)
            self.log.info('stopping container')
            container.stop()
            self.log.info('removing container')
            container.remove()
        except:
            self.log.info('no existing instance of measurement container found')

def parse_opts(opts):
    params = {}
    opts_list = opts.split(";")
    for opt in opts_list:
        key, val = opt.split("=")   
        params[key] = val
    return params

"""
Example: ./executor.py --jobid="test-id" --mode=docker --workdir="/home/leotest/leotest-testbed-v2/job_workdir"
"""
def main():
    log = logging.getLogger()  # root logger

    parser = argparse.ArgumentParser(description='Leotest executor.')

    # server mode options 
    parser.add_argument('--taskid', metavar='TASKID', type=str, required=False,
                                    help='taskid that triggered the executor')
    parser.add_argument('--runid', metavar='RUNID', type=str, required=False,
                                    help='runid that triggered the executor')
    parser.add_argument('--server', action='store_true')
    parser.add_argument('--no-server', dest='server', action='store_false')
    parser.set_defaults(server=False)

    parser.add_argument('--ttl-secs', metavar='TTL', type=int, default=300,
                        required=False, help='ttl for the server')

    # Common options 
    parser.add_argument('--setup-server', action='store_true')
    parser.add_argument('--no-setup-server', dest='setup_server', action='store_false')
    parser.set_defaults(setup_server=False)

    parser.add_argument('--server-node', metavar='NODEID', type=str, required=False,
                                    help='nodeid on which to setup the server')

    parser.add_argument('--length-secs', metavar='LENGTH', type=int, 
                                    help='length of experiment')
    parser.add_argument('--jobid', metavar='JOBID', type=str, 
                                    help='jobid that triggered the executor')
    parser.add_argument('--userid', metavar='USERID', type=str, 
                                    help='userid that owns the experiment')
    parser.add_argument('--nodeid', metavar='NODEID', type=str, 
                                    help='nodeid that triggered the executor')
    parser.add_argument('--access-token', metavar='TOKEN', type=str, 
                                    help='access token to authorize nodeid')
    parser.add_argument('--start-date', metavar='START_DATE', type=str, 
                                    help='start time of the job')
    parser.add_argument('--end-date', metavar='END_DATE', type=str, 
                                    help='end time of the job')
    parser.add_argument('--resched-buffer', metavar='SECONDS', type=int, 
                                help='time delta from the start time of the job' 
                                'after which to search for empty slots for job' 
                                'reschedule', default=1800)
    parser.add_argument('--grpc-hostname', metavar='HOST', type=str, 
                                    help='gRPC hostname', default='localhost')
    parser.add_argument('--grpc-port', metavar='PORT', type=int, 
                                    help='gRPC port', default=50051)                        
    parser.add_argument('--mode', metavar='MODE', type=str, choices=["docker"],
                        help='mode of execution.')
    parser.add_argument('--type', metavar='JOB_TYPE', type=str, choices=["cron", "atq"],
                        help='type of job: cron, atq.')
    
    overhead_parser = parser.add_mutually_exclusive_group(required=False)
    overhead_parser.add_argument('--overhead', dest='overhead', action='store_true')
    overhead_parser.add_argument('--no-overhead', dest='overhead', action='store_false')
    parser.set_defaults(overhead=True)

    parser.add_argument('--workdir', type=str, help='directory for file read/write')
    parser.add_argument('--executor-config', type=str, help='executor config yaml')     
    parser.add_argument('--deploy', type=str, required=False, help='deployment parameters')
    parser.add_argument('--execute', type=str, required=False, help='execution parameters')
    parser.add_argument('--finish', type=str, required=False, help='finish parameters')
    args = parser.parse_args()

    taskid = None 
    runid = None 
    jobid = args.jobid
    nodeid = args.nodeid
    userid = args.userid
    job_type = args.type 
    overhead = args.overhead
    start_date = args.start_date
    end_date = args.end_date
    access_token = args.access_token

    client = LeotestClient(grpc_hostname=args.grpc_hostname, 
                            grpc_port=args.grpc_port,
                            userid=nodeid, access_token=access_token)

    res = client.get_config()
    config = MessageToDict(res)['config']
    connection_string = config['datastore']['blob']['connectionString']
    container = config['datastore']['blob']['container']

    result_dict = {}
    stack = [result_dict]
    jobs = client.get_jobs_by_nodeid(nodeid)

    if len(jobs.jobs) > 0:
        for job in jobs.jobs:
            fetched_job = client.get_job_by_id(job.id)
            serialized_job = MessageToDict(fetched_job)
            data = serialized_job["config"]
            lines = data.split('\n')
            job_id = fetched_job.id

            if job_id:
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
        connection_string = result_dict.get("connection_string").strip('"')
        container = result_dict.get("container").strip('"')

    elif (connection_string is None) or (container is None) or (len(jobs.jobs) < 1):
        res = client.get_config()
        config = MessageToDict(res)['config']

        connection_string = config['datastore']['blob']['connectionString']
        container = config['datastore']['blob']['container']
    
 
    server_ip = None 
    # if server has to be setup, fetch public_ip of the server 
    if args.setup_server and args.server_node:
        res = client.get_nodes(nodeid=args.server_node)
        msg = MessageToDict(res)['nodes']
        if len(msg) > 0:
            if 'publicIp' in msg[0]:
                server_ip = msg[0]['publicIp']
            else:
                log.info('server with nodeid=%s has no public_ip' % args.server_node) 
        else:
            log.info('server with nodeid=%s not found' % args.server_node)

    # get scavenger status for this node 
    res = client.get_scavenger_status(nodeid)
    msg = MessageToDict(res)

    log.info(msg)
    terminate = {
        'flag': False,
        'status': 'None',
        'reason': 'None'
    }

    if 'found' in msg and msg['found']==True:
        scavenger_mode_active = msg['scavengerModeActive']
        log.info('scavenger_mode_active=%s, job_type=%s' 
                                       % (scavenger_mode_active, job_type))
        
        if scavenger_mode_active and overhead:
            terminate['flag'] = True
            terminate['status'] = 'TERMINATED'
            terminate['reason'] = 'job run terminated: scavenger mode is active.'

            if job_type == "atq":
                # reschedule 
                # reschedule after 30mins 
                starttime = time_now() + timedelta(seconds=int(args.resched_buffer))
                endtime = args.end_date
                ret = client.reschedule_job_nearest(
                                            jobid, str(starttime), str(endtime))

                # message message_reschedule_job_response {
                #     bool rescheduled = 1; 
                #     string message = 2; 
                # }

                msg = MessageToDict(ret)
                if 'rescheduled' in msg and msg['rescheduled']:
                    log.info('atq job rescheduled (scavenger mode is active).' 
                                                  'message=%s' % (msg['message']))
                    
                    terminate['flag'] = True
                    terminate['status'] = 'RESCHEDULED'
                    terminate['reason'] = 'job run rescheduled (scavenger mode is active); reason: %s' % msg['message']
                
                else:
                    log.info('atq job reschedule failed (scavenger mode is active).' 
                            'message=%s' % (msg['message']))
                    
                    terminate['flag'] = False
                    terminate['status'] = 'RESCHEDULE_FAILED'
                    terminate['reason'] = 'job run reschedule failed (scavenger mode is active); reason: %s' % msg['message']
                    
        else:
            log.info('scavenger_mode is not active, continuing execution.')

    else:
        log.warning('nodeid not found while fetching scavenger status. Continuing execution.')

    runid_server = None 
    if not args.server:

        timenow = time_now()
        # t = str(timenow.time()).replace(':', '-').replace('.', '-')
        runid = uuid.uuid4().hex
    
    else: 
        taskid = args.taskid 
        runid = args.runid 
        runid_server = '%s_server' % args.runid
        # get date from runid
        res = MessageToDict(client.get_runs(runid=runid))
        if 'runs' in res:
            timenow = datetimeParse(res['runs'][0]['startTime'])
            nodeid = res['runs'][0]['nodeid']
            userid = res['runs'][0]['userid']
        else:
            timenow = time_now()


    # cache executor session
    sessionstore = memcache_client('memcached:11211')

    try:
        sessionstore.set(key="%s_executor" % runid, value='1', expire=int(args.length_secs))
    except Exception as e:
        log.info(e)

    expdir = os.path.join(nodeid, jobid, str(timenow.year), str(timenow.month), 
                                str(timenow.day), runid)

    expdir_upload = os.path.join(nodeid, jobid, str(timenow.year), str(timenow.month), 
                                str(timenow.day))

    remote_path = os.path.join(config['datastore']['blob']['artifactPath'], expdir_upload)
    remote_path_job = os.path.join(config['datastore']['blob']['artifactPath'], 
                                                    nodeid, jobid)

    workdir = os.path.join(args.workdir, expdir)

    if not os.path.exists(workdir):
        os.makedirs(workdir)

    if args.server:
        logname = 'executor_server'
    else:
        logname = 'executor'

    # setup logging
    executor_log_stdout = open(os.path.join(workdir, 
                                                logname + '.stdout'), 'w')
    executor_log_stderr = open(os.path.join(workdir, 
                                                logname + '.stderr'), 'w')
    sys.stdout = executor_log_stdout
    sys.stderr = executor_log_stderr

    # logging.basicConfig(
    #     stream=sys.stdout,
    #     level=logging.INFO, 
    #     format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
    # log = logging.getLogger(__name__) 
    fileh = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
    fileh.setFormatter(formatter)

    # log = logging.getLogger()  # root logger
    for hdlr in log.handlers[:]:  # remove all old handlers
        log.removeHandler(hdlr)
    log.addHandler(fileh)      # set the new handler


    log.info('remote_path %s' % (remote_path))

    experiment_config = os.path.join(remote_path_job, "experiment-config.yaml")
    experiment_args = os.path.join(remote_path_job, "experiment-args.json")
    # executor_config = os.path.join(remote_path, "executor-config.yaml")
    executor_config = args.executor_config

    experiment_config_dst = os.path.join(workdir, "experiment-config.yaml")
    executor_config_dst = os.path.join(workdir, "executor-config.yaml")
    experiment_args_dst = os.path.join(workdir, "experiment-args.json")

    # for backwards compatibility, check if the config already exists in the storage    
    azclient = StorageDirectoryClient(connection_string, container)
    exp_args_from_orch = None
    if azclient.check_blob_exists(experiment_config):
        # upload config files to blob storage 
        log.info('downloading config from blob storage (%s:%s) --> %s' 
                            % (container, experiment_config, workdir))
        download_file(connection_string, container, experiment_config, experiment_config_dst)
    
    else:
        # get the config from the orchestrator 
        log.info('fetching experiment config from cloud orchestrator --> %s' 
               % experiment_config_dst)

        res = client.get_job_by_id(jobid)
        exp_args_from_orch = MessageToDict(res)
        exp_config = exp_args_from_orch['config']
        # write to file 
        with open(experiment_config_dst, 'w') as f:
            f.write(exp_config)

    if azclient.check_blob_exists(experiment_args):
        log.info('downloading args from blob storage (%s:%s) --> %s' 
                            % (container, experiment_args, workdir))
        download_file(connection_string, container, experiment_args, experiment_args_dst)
    else:
        log.info('fetching experiment args from cloud orchestrator --> %s' 
               % experiment_args_dst)

        if not exp_args_from_orch:
            res = client.get_job_by_id(jobid)
            exp_args_from_orch = MessageToDict(res)
        
        if 'config' in exp_config:
            exp_args_from_orch.pop('config')
        
        # dump rest of it as json 
        with open(experiment_args_dst, 'w') as f:
            json.dump(exp_args_from_orch, f)
        
    # log.info('download config from blob storage (%s:%s) --> %s'
    #                     % (container, executor_config, workdir))
    # download_file(connection_string, container, executor_config, executor_config_dst)
    log.info('copying executor configuration to jobdir %s --> %s' 
                                    % (executor_config, executor_config_dst))
    
    shutil.copy(executor_config, executor_config_dst)

    with open(experiment_config_dst, "r") as stream:
        try:
            experiment_config_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    
    with open(executor_config_dst, "r") as stream:
        try:
            executor_config_dict = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
   
    #experiment specific cloud storage account
    connection_string = experiment_config_dict["cloud_config"]["connection_string"]
    container = experiment_config_dict["cloud_config"]["container"]

    
    params = {
        'server_mode': args.server,
        'setup_server': args.setup_server,
        'server_node': args.server_node,
        'server_ip': server_ip,
        'ttl_secs': args.ttl_secs,
        'start_date': args.start_date,
        'end_date': args.end_date,
        'length_secs': args.length_secs,
        'taskid': taskid,
        'runid': runid,
        'runid_server': runid_server,
        'nodeid': nodeid,
        'jobid': jobid, 
        'userid': userid,
        'connection_string': connection_string,
        'container': container,
        'remote_path': remote_path,
        'workdir': workdir, 
        'expdir': expdir,
        'experiment': experiment_config_dict,
        'executor': executor_config_dict,
        'client': client,
        'start_time': str(timenow),
        'log': log,
        'executor_log_stdout': executor_log_stdout,
        'executor_log_stderr': executor_log_stderr,
        'terminate': terminate,
        'overhead': overhead,
        'job_type': job_type,
        'sessionstore': sessionstore
    }

    # if args.deploy:
    #     params['deploy'] = parse_opts(args.deploy)
    # else:
    #     params['deploy'] = {}
    
    # if args.execute:
    #     params['execute'] = parse_opts(args.execute)
    # else:
    #     params['execute'] = {}
    
    # if args.finish:
    #     params['finish'] = parse_opts(args.finish)
    # else:
    #     params['finish'] = {}
    
    print(params)
    if args.mode=="docker":
        executor = LeotestExecutorDocker(params=params)
    
    executor.run()

    try:
        sessionstore.delete(key="%s_executor" % runid)
    except Exception as e:
        log.info(e)

if __name__ == "__main__":
    main()
