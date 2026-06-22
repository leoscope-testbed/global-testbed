import json
import os
import time
import yaml
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
from common import config as cfg

import common.leotest_pb2_grpc as pb2_grpc
import common.leotest_pb2 as pb2

from node.trigger import LeotestTriggerMode, LeotestDockerNetworkMonitor, \
    LeotestGrpcMonitor, LeotestSatelliteMonitor, LeotestWeatherMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def kill_all_jobs(client, nodeid, sessionstore, resched_buffer=cfg.SCHEDULER_RESCHED_BUFFER_SECS):
    """Stop all running LEOScope overhead containers; abort CRON, reschedule ATQ."""
    log.info("[scheduler][kill_all_jobs] scavenger eviction triggered: nodeid=%s resched_buffer=%ds",
             nodeid, resched_buffer)
    docker_client = docker.from_env()

    filters = {
        "label": ["leotest=true", "overhead=true"],
        "status": "running",
    }
    try:
        container_list = docker_client.containers.list(filters=filters)
        log.info("[scheduler][kill_all_jobs] found %d overhead containers to evict", len(container_list))

        for container in container_list:
            labels = container.labels
            runid    = labels.get("runid", "unknown")
            jobid    = labels.get("jobid", "unknown")
            job_type = labels.get("type",  "unknown")
            end_date = labels.get("end_date", "")
            userid   = labels.get("userid", "")
            client_params = {
                "runid":      runid,
                "jobid":      jobid,
                "nodeid":     nodeid,
                "userid":     userid,
                "start_time": labels.get("start_time", ""),
            }
            log.info("[scheduler][kill_all_jobs] evicting container: runid=%s jobid=%s type=%s",
                     runid, jobid, job_type)

            executor_session_key = "%s_executor" % runid
            session_val = sessionstore.get(key=executor_session_key)
            log.info("[scheduler][kill_all_jobs] memcached session for runid=%s → %s",
                     runid, session_val)

            log.info("[scheduler][kill_all_jobs] stopping container runid=%s", runid)
            container.stop()
            container.remove()
            log.info("[scheduler][kill_all_jobs] container stopped runid=%s", runid)

            # Wait for the executor to finish its artifact-upload teardown before
            # overwriting the run status. Avoids a race where our RESCHEDULED write
            # gets clobbered by the executor's COMPLETE write a moment later.
            tries = 100
            while sessionstore.get(key=executor_session_key) is not None and tries > 0:
                time.sleep(1)
                log.info("[scheduler][kill_all_jobs] waiting for executor teardown "
                         "runid=%s tries_remaining=%d", runid, tries)
                tries -= 1

            if job_type == "cron":
                log.info("[scheduler][kill_all_jobs] ABORTING cron run runid=%s jobid=%s",
                         runid, jobid)
                client.update_run(
                    status="ABORTED",
                    status_message="Job aborted: scavenger mode is active.",
                    **client_params)

            elif job_type == "atq":
                starttime = time_now() + timedelta(seconds=int(resched_buffer))
                log.info("[scheduler][kill_all_jobs] RESCHEDULING atq run: "
                         "runid=%s jobid=%s new_start=%s end_date=%s",
                         runid, jobid, starttime, end_date)
                try:
                    ret = client.reschedule_job_nearest(jobid, str(starttime), str(end_date))
                    msg = MessageToDict(ret)
                except Exception:
                    log.exception("[scheduler][kill_all_jobs] reschedule RPC failed "
                                  "runid=%s jobid=%s", runid, jobid)
                    msg = {}

                if msg.get("rescheduled"):
                    log.info("[scheduler][kill_all_jobs] atq job rescheduled: "
                             "runid=%s jobid=%s message=%s", runid, jobid, msg.get("message"))
                    status  = "RESCHEDULED"
                    reason  = "job rescheduled (scavenger mode active): %s" % msg.get("message", "")
                else:
                    log.error("[scheduler][kill_all_jobs] atq job RESCHEDULE FAILED: "
                              "runid=%s jobid=%s message=%s", runid, jobid, msg.get("message"))
                    status  = "RESCHEDULE_FAILED"
                    reason  = "reschedule failed (scavenger mode active): %s" % msg.get("message", "")

                client.update_run(status=status, status_message=reason, **client_params)

            else:
                log.warning("[scheduler][kill_all_jobs] unrecognised job_type=%s "
                            "runid=%s jobid=%s — no run status update", job_type, runid, jobid)

    except Exception:
        log.exception("[scheduler][kill_all_jobs] EXCEPTION during eviction nodeid=%s", nodeid)


def kill_task_docker(runid, jobid):
    """Stop the server-mode experiment container for a given run."""
    log.info("[scheduler][kill_task_docker] stopping server container runid=%s jobid=%s",
             runid, jobid)
    docker_client = docker.from_env()
    filters = {
        "label": ["runid=%s" % runid, "jobid=%s" % jobid, "server=true"],
        "status": "running",
    }
    try:
        container_list = docker_client.containers.list(filters=filters)
        log.info("[scheduler][kill_task_docker] found %d matching containers runid=%s jobid=%s",
                 len(container_list), runid, jobid)
        if not container_list:
            log.warning("[scheduler][kill_task_docker] no server container found "
                        "runid=%s jobid=%s", runid, jobid)
            return
        container = container_list[0]
        log.info("[scheduler][kill_task_docker] stopping container name=%s", container.name)
        container.stop()
        log.info("[scheduler][kill_task_docker] server container stopped runid=%s jobid=%s",
                 runid, jobid)
    except Exception:
        log.exception("[scheduler][kill_task_docker] error stopping container "
                      "runid=%s jobid=%s", runid, jobid)


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


class LeotestTaskSchedulerPopen(LeotestTaskScheduler):
    """Execute a SERVER_START task by spawning a subprocess executor in server mode."""

    def __init__(self, executor_path,
                 module_name,
                 workdir,
                 nodeid,
                 artifactdir="/artifacts/",
                 grpc_hostname="localhost",
                 grpc_port=50051,
                 executor_config="/executor-config.yaml",
                 access_token=""):
        super().__init__(executor_path=executor_path)
        self.module_name   = module_name
        self.workdir       = workdir
        self.artifactdir   = artifactdir
        self.grpc_hostname = grpc_hostname
        self.grpc_port     = grpc_port
        self.executor_config = executor_config
        self.nodeid        = nodeid
        self.access_token  = access_token

    def _add_task(self, leotest_task):
        cmd = [self.executor_path]
        cmd += ["-m", self.module_name]
        cmd.append("--taskid=%s"        % leotest_task.get_taskid())
        cmd.append("--runid=%s"         % leotest_task.get_runid())
        cmd.append("--jobid=%s"         % leotest_task.get_jobid())
        cmd.append("--nodeid=%s"        % leotest_task.get_nodeid())
        cmd.append("--access-token=%s"  % self.access_token)
        cmd.append("--ttl-secs=%s"      % leotest_task.get_ttl_secs())
        cmd.append("--server")
        cmd.append("--workdir=%s"       % self.artifactdir)
        cmd.append("--grpc-hostname=%s" % self.grpc_hostname)
        cmd.append("--grpc-port=%s"     % self.grpc_port)
        cmd.append("--executor-config=%s" % self.executor_config)
        cmd.append("--mode=docker")

        log.info("[scheduler][task_add] spawning server task: taskid=%s runid=%s jobid=%s "
                 "nodeid=%s cmd=%s",
                 leotest_task.get_taskid(), leotest_task.get_runid(),
                 leotest_task.get_jobid(), leotest_task.get_nodeid(), cmd)
        Popen(cmd, cwd=self.workdir, preexec_fn=os.setsid)
        log.info("[scheduler][task_add] server task subprocess launched taskid=%s",
                 leotest_task.get_taskid())


class LeotestJobScheduler:
    """Base class to handle jobs sent by the orchestrator at the nodes."""

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
        """synchronize local job schedule with remote schedule"""

    def sync_jobs(self, remote_job_list):
        self._sync_jobs(remote_job_list)

    def _get_job_list(self):
        """return a list of jobs locally scheduled"""

    def get_job_list(self):
        return self._get_job_list()


class LeotestJobSchedulerAtq(LeotestJobScheduler):
    """Handle ATQ (one-shot) jobs from the orchestrator."""

    def __init__(self, executor_path,
                 nodeid,
                 artifactdir="/artifacts/",
                 grpc_hostname="localhost",
                 grpc_port=50051,
                 executor_config="/executor-config.yaml",
                 access_token=""):
        super().__init__(executor_path=executor_path)
        self.artifactdir     = artifactdir
        self.grpc_hostname   = grpc_hostname
        self.grpc_port       = grpc_port
        self.executor_config = executor_config
        self.nodeid          = nodeid
        self.access_token    = access_token

    def get_jobid_from_command(self, cmd):
        return cmd.decode("utf-8").split("#")[1].strip()

    def get_job_with_id(self, jobid):
        job = None
        job_found = False
        atq = atd.AtQueue()
        for _job in atq.jobs:
            if jobid == self.get_jobid_from_command(_job.command):
                log.info("[scheduler][atq] found atq entry for jobid=%s", jobid)
                job_found = True
                job = _job
                break
        return job_found, job

    def list_all_jobs(self):
        jobs = []
        atq = atd.AtQueue()
        for _job in atq.jobs:
            jobid = self.get_jobid_from_command(_job.command)
            jobs.append(jobid)
        return jobs

    def jobid_in_remote_list(self, jobid, remote_job_list):
        return any(jobid == job["jobid"] for job in remote_job_list)

    def _build_cmd(self, leotest_job):
        cmd = self.executor_path
        for key, value in leotest_job.get_job_params().items():
            cmd += " --%s='%s'" % (key, value)
        cmd += " --jobid=%s"        % leotest_job.get_jobid()
        cmd += " --start-date=%s"   % leotest_job.start_date
        cmd += " --end-date=%s"     % leotest_job.end_date
        cmd += " --resched-buffer=%d" % cfg.SCHEDULER_RESCHED_BUFFER_SECS
        cmd += " --length-secs=%s"  % leotest_job.get_length_secs()
        cmd += " --nodeid=%s"       % self.nodeid
        cmd += " --userid=%s"       % leotest_job.userid
        cmd += " --access-token=%s" % self.access_token
        cmd += ' --workdir="%s"'    % self.artifactdir
        cmd += " --grpc-hostname=%s" % self.grpc_hostname
        cmd += " --grpc-port=%s"    % self.grpc_port
        cmd += " --executor-config=%s" % self.executor_config
        cmd += " --mode=docker"
        cmd += " --type=%s"         % leotest_job.type
        if leotest_job.overhead:
            cmd += " --overhead"
        else:
            cmd += " --no-overhead"
        if leotest_job.server:
            cmd += " --setup-server"
            cmd += " --server-node=%s" % leotest_job.server
        logfile = os.path.join(self.artifactdir, "executor_stdout.log")
        cmd += " >> %s" % logfile
        return cmd

    def _add_job(self, leotest_job):
        cmd = self._build_cmd(leotest_job)
        start_obj = leotest_job.get_start_time_obj()

        if time_now() >= start_obj:
            log.warning("[scheduler][atq][add_job] skipping ATQ job — start time already passed: "
                        "jobid=%s start_date=%s", leotest_job.get_jobid(), leotest_job.start_date)
        else:
            log.info("[scheduler][atq][add_job] scheduling ATQ job: "
                     "jobid=%s start_date=%s cmd_prefix=%s...",
                     leotest_job.get_jobid(), leotest_job.start_date, cmd[:120])
            atd.at("%s # %s" % (cmd, leotest_job.get_jobid()), start_obj)
            log.info("[scheduler][atq][add_job] ATQ entry created jobid=%s", leotest_job.get_jobid())

    def _sync_jobs(self, remote_job_list):
        log.info("[scheduler][atq][sync] clearing existing ATQ entries")
        atq = atd.AtQueue()
        try:
            for job in atq.jobs:
                atd.atrm(job)
        except Exception:
            log.exception("[scheduler][atq][sync] error clearing ATQ entries")

        local_jobs = {}
        atq = atd.AtQueue()
        for job in atq.jobs:
            jobid = self.get_jobid_from_command(job.command)
            if not self.jobid_in_remote_list(jobid, remote_job_list):
                atd.atrm(job)
            else:
                local_jobs[jobid] = job

        log.info("[scheduler][atq][sync] syncing %d remote ATQ jobs", len(remote_job_list))
        for job in remote_job_list:
            jobid = job.jobid
            if jobid not in local_jobs:
                log.info("[scheduler][atq][sync] adding ATQ job jobid=%s", jobid)
                self.add_job(job)
            else:
                log.debug("[scheduler][atq][sync] ATQ job already scheduled jobid=%s", jobid)
        log.info("[scheduler][atq][sync] done")


class LeotestJobSchedulerCron(LeotestJobScheduler):
    """Handle cron (recurring) jobs from the orchestrator."""

    def __init__(self, executor_path,
                 nodeid,
                 artifactdir="/artifacts/",
                 grpc_hostname="localhost",
                 grpc_port=50051,
                 executor_config="/executor-config.yaml",
                 access_token=""):
        super().__init__(executor_path=executor_path)
        self.cron            = CronTab(user=True)
        self.artifactdir     = artifactdir
        self.grpc_hostname   = grpc_hostname
        self.grpc_port       = grpc_port
        self.executor_config = executor_config
        self.nodeid          = nodeid
        self.access_token    = access_token

    def _build_cmd(self, leotest_job):
        cmd = self.executor_path
        for key, value in leotest_job.get_job_params().items():
            cmd += " --%s='%s'" % (key, value)
        cmd += " --jobid=%s"         % leotest_job.get_jobid()
        cmd += " --start-date=%s"    % leotest_job.start_date
        cmd += " --end-date=%s"      % leotest_job.end_date
        cmd += " --resched-buffer=%d" % cfg.SCHEDULER_RESCHED_BUFFER_SECS
        cmd += " --length-secs=%s"   % leotest_job.get_length_secs()
        cmd += " --nodeid=%s"        % self.nodeid
        cmd += " --userid=%s"        % leotest_job.userid
        cmd += " --access-token=%s"  % self.access_token
        cmd += ' --workdir="%s"'     % self.artifactdir
        cmd += " --grpc-hostname=%s" % self.grpc_hostname
        cmd += " --grpc-port=%s"     % self.grpc_port
        cmd += " --executor-config=%s" % self.executor_config
        cmd += " --mode=docker"
        cmd += " --type=%s"          % leotest_job.type
        if leotest_job.overhead:
            cmd += " --overhead"
        else:
            cmd += " --no-overhead"
        if leotest_job.server:
            cmd += " --setup-server"
            cmd += " --server-node=%s" % leotest_job.server
        logfile = os.path.join(self.artifactdir, "executor_stdout.log")
        cmd += " >> %s" % logfile
        return cmd

    def _add_job(self, leotest_job):
        cmd = self._build_cmd(leotest_job)
        cron_str = leotest_job.get_cron_string()
        log.info("[scheduler][cron][add_job] adding cron entry: jobid=%s schedule=%s",
                 leotest_job.get_jobid(), cron_str)
        job = self.cron.new(command=cmd, comment=leotest_job.get_jobid())
        job.setall(cron_str)
        self.cron.write()
        log.info("[scheduler][cron][add_job] cron entry written jobid=%s", leotest_job.get_jobid())

    def _remove_job(self, leotest_job):
        log.info("[scheduler][cron][remove_job] removing cron entry jobid=%s",
                 leotest_job.get_jobid())
        self.cron.remove_all(comment=leotest_job.get_jobid())
        self.cron.write()

    def _sync_jobs(self, remote_leotest_jobs):
        log.info("[scheduler][cron][sync] clearing all cron entries and re-adding %d jobs",
                 len(remote_leotest_jobs))
        self.cron.remove_all()
        for leotest_job in remote_leotest_jobs:
            self.add_job(leotest_job)
        self.cron.write()
        log.info("[scheduler][cron][sync] crontab written")

    def get_params_from_cmd(self, cmd):
        params = {}
        tokens = cmd.split(" ")
        for i in range(1, len(tokens)):
            arg = tokens[i]
            arg_split = arg.split("=")
            key = arg_split[0][2:]
            val = arg_split[1]
            params[key] = val
        return params

    def _get_job_list(self):
        leotest_job_list = []
        for job in self.cron:
            jobid = job.comment
            job_params = self.get_params_from_cmd(job.command)
            leotest_job = LeotestJobCron(
                jobid=jobid,
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
    client.init_grpc_client()  # reset gRPC socket after fork()

    # ------------------------------------------------------------------
    # Heartbeat + node presence
    # ------------------------------------------------------------------
    log.info("[scheduler][tick] sending heartbeat nodeid=%s", nodeid)
    client.send_heartbeat(nodeid)
    public_ip = get_public_ip()
    client.update_node(nodeid=nodeid, public_ip=public_ip)
    log.info("[scheduler][tick] node presence updated nodeid=%s public_ip=%s", nodeid, public_ip)

    # ------------------------------------------------------------------
    # Job sync
    # ------------------------------------------------------------------
    log.info("[scheduler][tick] fetching jobs from orchestrator nodeid=%s", nodeid)
    res = MessageToDict(client.get_jobs_by_nodeid(nodeid, forever=True))
    log.debug("[scheduler][tick] raw job response: %s", res)

    remote_job_list = {"cron": [], "atq": []}

    if "jobs" in res:
        job_ids_seen = []
        for job in res["jobs"]:
            job_id = job["id"]
            log.info("[scheduler][tick][job] processing job: id=%s nodeid=%s type=%s",
                     job_id, job.get("nodeid"), job.get("type"))

            # --- save experiment config locally (use yaml.safe_load, not custom parser) ---
            try:
                retrieve_job = client.get_job_by_id(job_id)
                config_yaml = retrieve_job.config
                if config_yaml:
                    try:
                        result_dict = yaml.safe_load(config_yaml) or {}
                    except yaml.YAMLError:
                        log.exception("[scheduler][tick][job] YAML parse error for jobid=%s "
                                      "— storing raw string under 'raw'", job_id)
                        result_dict = {"raw": config_yaml}
                else:
                    result_dict = {}

                config_path = os.path.join(
                    cfg.EXPERIMENT_CONFIGS_DIR,
                    "experiment_config_%s.json" % job_id)
                os.makedirs(os.path.dirname(config_path), exist_ok=True)
                with open(config_path, "w") as f:
                    json.dump(result_dict, f, indent=2)
                log.info("[scheduler][tick][job] experiment config saved: jobid=%s path=%s "
                         "keys=%s", job_id, config_path, list(result_dict.keys()))
            except Exception:
                log.exception("[scheduler][tick][job] FAILED to fetch/save config for jobid=%s",
                              job_id)

            if nodeid != job["nodeid"]:
                log.debug("[scheduler][tick][job] skipping job not for this node: "
                          "jobid=%s job_nodeid=%s our_nodeid=%s",
                          job_id, job.get("nodeid"), nodeid)
                continue

            server   = job.get("server")
            trigger  = job.get("trigger")
            job_type = job.get("type", "cron").lower()
            overhead = job.get("overhead", False)
            job_ids_seen.append(job_id)
            log.info("[scheduler][tick][job] accepting job: jobid=%s type=%s overhead=%s "
                     "server=%s trigger=%s schedule=%s startDate=%s endDate=%s",
                     job_id, job_type, overhead, server, trigger,
                     job.get("schedule"), job.get("startDate"), job.get("endDate"))

            if job_type == "cron":
                leo_job = LeotestJobCron(
                    jobid=job_id,
                    nodeid=job["nodeid"],
                    userid=job["userid"],
                    start_date=job["startDate"],
                    end_date=job["endDate"],
                    server=server,
                    trigger=trigger,
                    length_secs=job["lengthSecs"],
                    job_params=job["params"],
                    overhead=overhead)
                leo_job.set_schedule_cron(job["schedule"])
                remote_job_list["cron"].append(leo_job)

            elif job_type == "atq":
                leo_job = LeotestJobAtq(
                    jobid=job_id,
                    nodeid=job["nodeid"],
                    userid=job["userid"],
                    start_date=job["startDate"],
                    end_date=job["endDate"],
                    server=server,
                    trigger=trigger,
                    length_secs=job["lengthSecs"],
                    job_params=job["params"],
                    overhead=overhead)
                remote_job_list["atq"].append(leo_job)

            else:
                log.warning("[scheduler][tick][job] unknown job_type=%s jobid=%s — skipping",
                            job_type, job_id)

        log.info("[scheduler][tick] job fetch complete: %d cron, %d atq for nodeid=%s "
                 "(total in response=%d accepted=%d)",
                 len(remote_job_list["cron"]), len(remote_job_list["atq"]),
                 nodeid, len(res["jobs"]), len(job_ids_seen))
    else:
        log.info("[scheduler][tick] no jobs returned from orchestrator for nodeid=%s", nodeid)

    log.info("[scheduler][tick] syncing cron scheduler: %d jobs", len(remote_job_list["cron"]))
    cron_scheduler.sync_jobs(remote_job_list["cron"])
    log.info("[scheduler][tick] syncing atq scheduler: %d jobs", len(remote_job_list["atq"]))
    atq_scheduler.sync_jobs(remote_job_list["atq"])
    for key in remote_job_list:
        trigger_module.sync_triggers(remote_job_list[key])
    log.info("[scheduler][tick] job sync complete nodeid=%s", nodeid)

    # ------------------------------------------------------------------
    # Task dispatch
    # ------------------------------------------------------------------
    log.info("[scheduler][tick] fetching tasks from orchestrator nodeid=%s", nodeid)
    res = MessageToDict(client.get_tasks(nodeid=nodeid))
    log.debug("[scheduler][tick] raw task response: %s", res)

    if "tasks" in res:
        log.info("[scheduler][tick] got %d tasks for nodeid=%s", len(res["tasks"]), nodeid)
        for task in res["tasks"]:
            task_id   = task.get("taskid", "unknown")
            task_type = task.get("type",   "unknown")
            task_status = task.get("status", "")
            log.info("[scheduler][tick][task] task: taskid=%s type=%s status=%s runid=%s jobid=%s",
                     task_id, task_type, task_status, task.get("runid"), task.get("jobid"))

            leo_task = LeotestTask(
                taskid=task["taskid"],
                runid=task["runid"],
                jobid=task["jobid"],
                nodeid=task["nodeid"],
                task_type=task["type"],
                ttl_secs=task["ttlSecs"])

            if task_status and task_status != "TASK_COMPLETE":
                exists = sessionstore.get(key=task_id, default=None)
                if not exists:
                    log.info("[scheduler][tick][task] new task — adding to sessionstore: "
                             "taskid=%s type=%s", task_id, task_type)
                    sessionstore.set(
                        key=task_id,
                        value="1",
                        expire=24 * 60 * 60)

                    if task_type == "SERVER_START":
                        log.info("[scheduler][tick][task] dispatching SERVER_START task "
                                 "taskid=%s runid=%s", task_id, task["runid"])
                        task_scheduler.add_task(leo_task)

                    elif task_type == "SERVER_STOP":
                        log.info("[scheduler][tick][task] handling SERVER_STOP: "
                                 "stopping container runid=%s jobid=%s taskid=%s",
                                 task["runid"], task["jobid"], task_id)
                        kill_task_docker(leo_task.get_runid(), leo_task.get_jobid())
                        client.update_task(taskid=task_id, status="TASK_COMPLETE")
                        log.info("[scheduler][tick][task] SERVER_STOP TASK_COMPLETE taskid=%s",
                                 task_id)

                    else:
                        log.warning("[scheduler][tick][task] unrecognised task_type=%s "
                                    "taskid=%s — ignoring", task_type, task_id)
                else:
                    log.debug("[scheduler][tick][task] task already in sessionstore — "
                              "skipping: taskid=%s", task_id)
            else:
                log.debug("[scheduler][tick][task] task already TASK_COMPLETE, skipping: "
                          "taskid=%s", task_id)
    else:
        log.info("[scheduler][tick] no tasks returned for nodeid=%s", nodeid)

    # ------------------------------------------------------------------
    # Scavenger mode check
    # ------------------------------------------------------------------
    log.info("[scheduler][tick] checking scavenger mode nodeid=%s", nodeid)
    try:
        scav_msg = MessageToDict(client.get_scavenger_status(nodeid))
        log.info("[scheduler][tick] scavenger status response: %s", scav_msg)
        if scav_msg.get("scavengerModeActive"):
            log.warning("[scheduler][tick] SCAVENGER MODE IS ACTIVE nodeid=%s — "
                        "evicting all overhead containers", nodeid)
            thread = threading.Thread(
                target=kill_all_jobs,
                args=[client, nodeid, sessionstore],
                daemon=True)
            thread.start()
        else:
            log.info("[scheduler][tick] scavenger mode not active nodeid=%s", nodeid)
    except Exception:
        log.exception("[scheduler][tick] FAILED to fetch scavenger status nodeid=%s", nodeid)


def scheduler_loop(nodeid,
                   grpc_hostname="localhost",
                   grpc_port=50051,
                   interval=cfg.SCHEDULER_INTERVAL_SECS,
                   workdir=cfg.WORKDIR,
                   artifactdir=cfg.ARTIFACTDIR,
                   executor_config=cfg.EXECUTOR_CONFIG,
                   access_token=""):

    cfg.log_config_summary()
    log.info("[scheduler] ===== LEOScope Scheduler Starting =====")
    log.info("[scheduler] nodeid=%s grpc=%s:%d interval=%ds workdir=%s artifactdir=%s",
             nodeid, grpc_hostname, grpc_port, interval, workdir, artifactdir)

    # ------------------------------------------------------------------
    # Memcached session store
    # ------------------------------------------------------------------
    memcached_addr = (cfg.MEMCACHED_HOST, cfg.MEMCACHED_PORT)
    log.info("[scheduler] connecting to memcached at %s:%d", cfg.MEMCACHED_HOST, cfg.MEMCACHED_PORT)
    sessionstore = memcache_client(memcached_addr)

    # ------------------------------------------------------------------
    # Job schedulers
    # ------------------------------------------------------------------
    exec_hook = "cd %s && %s -m node.executor" % (workdir, cfg.PYTHON_BIN)
    log.info("[scheduler] executor hook: %s", exec_hook)

    cron_scheduler = LeotestJobSchedulerCron(
        executor_path=exec_hook,
        nodeid=nodeid,
        artifactdir=artifactdir,
        grpc_hostname=grpc_hostname,
        grpc_port=grpc_port,
        executor_config=executor_config,
        access_token=access_token)

    atq_scheduler = LeotestJobSchedulerAtq(
        executor_path=exec_hook,
        nodeid=nodeid,
        artifactdir=artifactdir,
        grpc_hostname=grpc_hostname,
        grpc_port=grpc_port,
        executor_config=executor_config,
        access_token=access_token)

    task_scheduler = LeotestTaskSchedulerPopen(
        executor_path=cfg.PYTHON_BIN,
        module_name="node.executor",
        workdir=workdir,
        nodeid=nodeid,
        artifactdir=artifactdir,
        grpc_hostname=grpc_hostname,
        grpc_port=grpc_port,
        executor_config=executor_config,
        access_token=access_token)

    # ------------------------------------------------------------------
    # gRPC client + initial config fetch
    # ------------------------------------------------------------------
    client = LeotestClient(
        grpc_hostname=grpc_hostname,
        grpc_port=grpc_port,
        userid=nodeid,
        access_token=access_token)

    log.info("[scheduler] fetching global config from orchestrator")
    try:
        res = client.get_config()
        config = MessageToDict(res).get("config", {})
        weather_api_key = config.get("weather", {}).get("apikey", "")
        log.info("[scheduler] global config fetched ok; weather_api_key=<redacted if set: %s>",
                 "yes" if weather_api_key else "no")
    except Exception:
        log.exception("[scheduler] FAILED to fetch global config from orchestrator")
        weather_api_key = ""

    # ------------------------------------------------------------------
    # Node registration check + coordinates
    # ------------------------------------------------------------------
    log.info("[scheduler] fetching node info for nodeid=%s", nodeid)
    try:
        res = client.get_nodes(nodeid=nodeid)
        msg = MessageToDict(res)
        log.info("[scheduler] node response: %s", msg)

        if "nodes" in msg and len(msg["nodes"]) >= 1:
            coords = msg["nodes"][0].get("coords", "")
            log.info("[scheduler] node coords: %s", coords)
        else:
            log.error("[scheduler] node not registered with orchestrator: nodeid=%s — exiting", nodeid)
            exit(1)
    except Exception:
        log.exception("[scheduler] FAILED to fetch node info for nodeid=%s — exiting", nodeid)
        exit(1)

    lat, lon = coords.split(",")
    log.info("[scheduler] node location: lat=%s lon=%s", lat, lon)

    # ------------------------------------------------------------------
    # Trigger monitors
    # ------------------------------------------------------------------
    trigger_module = LeotestTriggerMode()

    dockermon = LeotestDockerNetworkMonitor(trigger_module)
    log.info("[scheduler] docker network monitor initialised")

    i_api, i_lat, i_lon, i_ele = get_weather_mon_info()

    satmon = LeotestSatelliteMonitor(trigger_module, name=nodeid, lat=lat, lon=lon, ele=i_ele)
    log.info("[scheduler] satellite monitor initialised; starting async")
    satmon.run_async()

    weathermon = LeotestWeatherMonitor(
        trigger_module, api=i_api, lat=i_lat, lon=i_lon, api_key=weather_api_key)
    log.info("[scheduler] weather monitor initialised; starting async")
    weathermon.run_async()

    # ------------------------------------------------------------------
    # Wait for Starlink terminal discovery
    # ------------------------------------------------------------------
    log.info("[scheduler] waiting for Starlink terminal discovery via Redis %s:%d",
             cfg.REDIS_HOST, cfg.REDIS_PORT)
    r = redis.Redis(host=cfg.REDIS_HOST, port=cfg.REDIS_PORT, db=cfg.REDIS_DB, decode_responses=True)

    while True:
        dish_status = r.get("starlink-terminal-found")
        log.info("[scheduler] dish_status=%s", dish_status)
        if not dish_status:
            time.sleep(1)
            log.info("[scheduler] waiting for Starlink terminal (dish) status from Redis...")
        else:
            if dish_status == "True":
                utid = r.get("starlink-terminal-id")
                log.info("[scheduler] Starlink terminal found utid=%s — starting gRPC monitor", utid)
                grpcmon = LeotestGrpcMonitor(
                    trigger_module, utid,
                    fields=["uplink_throughput_bps",
                            "downlink_throughput_bps",
                            "pop_ping_latency_ms",
                            "direction_azimuth",
                            "direction_elevation",
                            "currently_obstructed",
                            "fraction_obstructed"])
                grpcmon.run(run_async=True)
                log.info("[scheduler] gRPC monitor started for utid=%s", utid)
            else:
                log.info("[scheduler] no Starlink terminal found — skipping gRPC monitor")
            break

    # ------------------------------------------------------------------
    # Main scheduler loop
    # ------------------------------------------------------------------
    log.info("[scheduler] entering main scheduler loop: interval=%ds", interval)
    tick = 0
    while True:
        tick += 1
        log.info("[scheduler] ===== TICK #%d nodeid=%s =====", tick, nodeid)
        args = (nodeid, client, cron_scheduler, atq_scheduler, task_scheduler,
                sessionstore, trigger_module, log)
        p = Process(target=_scheduler_execute, args=args)
        p.start()
        p.join(60)
        if p.is_alive():
            log.warning("[scheduler] tick #%d timed out after 60s — terminating subprocess", tick)
            p.terminate()
        else:
            log.info("[scheduler] tick #%d completed exitcode=%s", tick, p.exitcode)
        time.sleep(interval)
