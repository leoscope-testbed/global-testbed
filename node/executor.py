#!/usr/bin/python3
"""
Experiment lifecycle manager: deploy → execute → collect artifacts → upload to Azure Blob Storage.

Invoked by the node scheduler (cron or atd). Each invocation owns exactly one run.
"""

import os
import sys
import uuid
import yaml
import json
import shutil
import docker
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
from common.azure import download_file
from common.utils import route, time_now, TerminalGrpcDataCsv, make_archive, StorageDirectoryClient
from common import config as cfg

import common.leotest_pb2_grpc as pb2_grpc
import common.leotest_pb2 as pb2

# ---------------------------------------------------------------------------
# Logging is configured in main() once the per-run log file is open.
# Use the root logger here so that the handler swap in main() takes effect.
# ---------------------------------------------------------------------------
log = logging.getLogger(__name__)


def _container_is_running(docker_client, container_name: str) -> bool:
    """Return True if container exists and is running; False if exited; raises on other errors."""
    try:
        return docker_client.containers.get(container_name).status == "running"
    except docker.errors.NotFound:
        return False


def check_task_status(taskid: str, client: LeotestClient) -> str:
    """Poll the orchestrator for a task's status string (e.g. 'TASK_COMPLETE')."""
    res = MessageToDict(client.get_tasks(taskid=taskid))
    log.debug("[task_poll] taskid=%s response=%s", taskid, res)
    if "tasks" in res and res["tasks"]:
        return res["tasks"][0].get("status", "")
    return ""


class LeotestExecutor:
    """Base lifecycle manager for one experiment run."""

    def __init__(self, params: dict):
        self.params = params
        self.log = params["log"]

        if not self.params["server_mode"]:
            self.client_params = {
                "runid": self.params["runid"],
                "jobid": self.params["jobid"],
                "nodeid": self.params["nodeid"],
                "userid": self.params["userid"],
                "start_time": self.params["start_time"],
            }
        else:
            self.client_params = {
                "runid": self.params["runid_server"],
                "jobid": self.params["jobid"],
                "nodeid": self.params["nodeid"],
                "userid": self.params["userid"],
                "start_time": self.params["start_time"],
            }

        self._route_enabled = False
        if "route" in params["executor"]:
            self._route_enabled = True
            self._route_ip = params["executor"]["route"]["ip"]
            self._route_gw = params["executor"]["route"]["gateway"]
            self._route_dev = params["executor"]["route"]["dev"]
            self.log.info("[executor] route configured: dst=%s gw=%s dev=%s",
                          self._route_ip, self._route_gw, self._route_dev)

        self.executor_log_stdout = params["executor_log_stdout"]
        self.executor_log_stderr = params["executor_log_stderr"]

        grpc_path = cfg.STARLINK_GRPC_TOOLS_PATH
        grpc_csv_file = "%s/grpc.csv" % self.params["workdir"]
        self.log.info("[executor] initialising Starlink gRPC telemetry poll: "
                      "api_path=%s csv=%s", grpc_path, grpc_csv_file)
        self.terminal_grpc_poll = TerminalGrpcDataCsv(
            api_path=grpc_path,
            logfile=grpc_csv_file)
        self.terminal_grpc_poll.run(_async=True)
        self.log.info("[executor] Starlink gRPC telemetry poll started asynchronously")

    # ------------------------------------------------------------------
    # deploy
    # ------------------------------------------------------------------
    def _deploy_job(self):
        """Subclass hook — prepare environment before execution."""

    def deploy_job(self):
        runid = self.client_params["runid"]
        jobid = self.client_params["jobid"]
        self.log.info("[executor][deploy] START runid=%s jobid=%s server_mode=%s",
                      runid, jobid, self.params["server_mode"])

        if (not self.params["server_mode"]) and self.params["setup_server"]:
            server_node = self.params["server_node"]
            self.log.info("[executor][deploy] requesting SERVER_START on nodeid=%s "
                          "runid=%s ttl_secs=%d", server_node, runid, self.params["length_secs"])
            self.params["client"].update_run(
                status="DEPLOYING",
                status_message="Setting up server node %s" % server_node,
                **self.client_params)

            taskid = uuid.uuid4().hex
            timeout = self.params["length_secs"]
            self.params["client"].schedule_task(
                taskid=taskid,
                runid=self.params["runid"],
                jobid=self.params["jobid"],
                nodeid=server_node,
                _type="SERVER_START",
                ttl_secs=timeout)
            self.log.info("[executor][deploy] SERVER_START task created taskid=%s; "
                          "polling for TASK_COMPLETE (timeout=%ds)", taskid, timeout)

            poll_interval = 3
            elapsed = 0
            while elapsed < timeout:
                status = check_task_status(taskid, self.params["client"])
                self.log.info("[executor][deploy] server task poll: taskid=%s status=%s "
                              "elapsed=%ds timeout=%ds", taskid, status, elapsed, timeout)
                if status == "TASK_COMPLETE":
                    self.log.info("[executor][deploy] server node ready taskid=%s elapsed=%ds",
                                  taskid, elapsed)
                    break
                sleep(poll_interval)
                elapsed += poll_interval
            else:
                self.log.error("[executor][deploy] SERVER_START timed out after %ds "
                               "taskid=%s — proceeding without confirmed server", timeout, taskid)

        if self._route_enabled:
            self.log.info("[executor][deploy] adding host route dst=%s gw=%s dev=%s",
                          self._route_ip, self._route_gw, self._route_dev)
            route("del", self._route_ip, self._route_gw, self._route_dev)
            route("add", self._route_ip, self._route_gw, self._route_dev)
            self.log.info("[executor][deploy] host route applied")

        self._deploy_job()
        self.log.info("[executor][deploy] DONE runid=%s jobid=%s", runid, jobid)

    # ------------------------------------------------------------------
    # execute
    # ------------------------------------------------------------------
    def _execute_job(self):
        """Subclass hook — run the experiment."""

    def execute_job(self):
        runid = self.client_params["runid"]
        jobid = self.client_params["jobid"]
        self.log.info("[executor][execute] START runid=%s jobid=%s", runid, jobid)
        self.params["client"].update_run(
            status="EXECUTING",
            status_message="Executing job.",
            **self.client_params)
        try:
            self._execute_job()
            self.log.info("[executor][execute] DONE runid=%s jobid=%s", runid, jobid)
        except Exception:
            self.log.exception("[executor][execute] EXCEPTION during job execution "
                               "runid=%s jobid=%s", runid, jobid)

    # ------------------------------------------------------------------
    # finish / artifact upload
    # ------------------------------------------------------------------
    def _finish_job(self):
        """Subclass hook — cleanup after execution."""

    def finish_job(self):
        runid = self.client_params["runid"]
        jobid = self.client_params["jobid"]
        self.log.info("[executor][finish] START runid=%s jobid=%s", runid, jobid)

        # --- stop server if we started one ---
        if (not self.params["server_mode"]) and self.params["setup_server"]:
            server_node = self.params["server_node"]
            self.log.info("[executor][finish] sending SERVER_STOP to nodeid=%s runid=%s",
                          server_node, runid)
            self.params["client"].update_run(
                status="FINISHING",
                status_message="Stopping server node %s" % server_node,
                **self.client_params)
            taskid = uuid.uuid4().hex
            self.params["client"].schedule_task(
                taskid=taskid,
                runid=self.params["runid"],
                jobid=self.params["jobid"],
                nodeid=server_node,
                _type="SERVER_STOP",
                ttl_secs=self.params["length_secs"])
            self.log.info("[executor][finish] SERVER_STOP task created taskid=%s", taskid)

        # --- remove host route ---
        if self._route_enabled:
            self.log.info("[executor][finish] removing host route dst=%s gw=%s dev=%s",
                          self._route_ip, self._route_gw, self._route_dev)
            route("del", self._route_ip, self._route_gw, self._route_dev)
            self.log.info("[executor][finish] host route removed")

        # --- stop telemetry ---
        self.log.info("[executor][finish] stopping Starlink gRPC telemetry poll")
        self.terminal_grpc_poll.stop()

        # --- archive ---
        workdir = self.params["workdir"]
        archive_path = "%s.zip" % workdir
        if self.params["server_mode"]:
            archive_path_remote = "%s/%s_server.zip" % (
                self.params["remote_path"], self.params["runid"])
        else:
            archive_path_remote = "%s/%s.zip" % (
                self.params["remote_path"], self.params["runid"])

        self.log.info("[executor][finish] archiving workdir=%s → %s", workdir, archive_path)
        make_archive(workdir, archive_path)
        self.log.info("[executor][finish] archive created: %s (%.2f MB)",
                      archive_path, os.path.getsize(archive_path) / (1024 * 1024))

        # --- close log files before upload so they are included in the archive ---
        self.log.info("[executor][finish] flushing executor log files before upload")
        self.params["client"].update_run(
            status="FINISHING",
            status_message="Uploading artifacts to Azure Blob Storage.",
            **self.client_params)

        # Redirect logging back to stdout so we keep visibility after log files close
        fileh = logging.StreamHandler(stream=sys.__stdout__)
        formatter = logging.Formatter(
            "%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
        fileh.setFormatter(formatter)
        root = logging.getLogger()
        for hdlr in root.handlers[:]:
            root.removeHandler(hdlr)
        root.addHandler(fileh)
        self.log = root  # keep using it

        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        self.executor_log_stdout.close()
        self.executor_log_stderr.close()

        # --- upload ---
        connection_string = self.params["connection_string"]
        container = self.params["container"]
        self.log.info("[executor][finish][upload] START container=%s remote_path=%s",
                      container, archive_path_remote)
        try:
            azclient = StorageDirectoryClient(connection_string, container)
            azclient.upload_file(archive_path, archive_path_remote)
            self.log.info("[executor][finish][upload] upload complete: %s", archive_path_remote)
        except Exception:
            self.log.exception("[executor][finish][upload] FAILED to upload artifact "
                               "runid=%s archive=%s remote=%s container=%s",
                               runid, archive_path, archive_path_remote, container)
            raise

        # --- SAS URL ---
        try:
            blob_url = azclient.get_sas_url(archive_path_remote)
            self.log.info("[executor][finish][upload] SAS URL generated runid=%s url_prefix=%s...",
                          runid, blob_url[:80] if blob_url else "")
        except Exception:
            self.log.exception("[executor][finish][upload] FAILED to generate SAS URL "
                               "runid=%s remote=%s", runid, archive_path_remote)
            blob_url = ""

        # --- update run with blob URL and clearing status ---
        self.params["client"].update_run(
            blob_url=blob_url,
            status="FINISHING",
            status_message="Clearing local artifacts.",
            **self.client_params)

        # --- clean up local files ---
        self.log.info("[executor][finish] removing local archive %s", archive_path)
        try:
            os.remove(archive_path)
        except Exception:
            self.log.warning("[executor][finish] could not remove archive %s", archive_path)
        self.log.info("[executor][finish] removing local workdir %s", workdir)
        try:
            shutil.rmtree(workdir)
        except Exception:
            self.log.warning("[executor][finish] could not remove workdir %s", workdir)

        self._finish_job()

        end_time = str(time_now())
        self.log.info("[executor][finish] marking run COMPLETE runid=%s jobid=%s end_time=%s",
                      runid, jobid, end_time)
        self.params["client"].update_run(
            end_time=end_time,
            blob_url=blob_url,
            status="COMPLETE",
            status_message="Done executing the job.",
            **self.client_params)
        self.log.info("[executor][finish] DONE runid=%s jobid=%s", runid, jobid)

    # ------------------------------------------------------------------
    def run(self):
        if self.params["terminate"]["flag"]:
            runid = self.client_params["runid"]
            status = self.params["terminate"]["status"]
            reason = self.params["terminate"]["reason"]
            self.log.warning("[executor] run TERMINATED before start: "
                             "runid=%s status=%s reason=%s", runid, status, reason)
            self.params["client"].update_run(
                status=status,
                status_message=reason,
                **self.client_params)
        else:
            self.deploy_job()
            self.execute_job()
            self.finish_job()


class LeotestExecutorDocker(LeotestExecutor):
    """Docker-based executor: pulls image, runs container, streams logs, enforces TTL."""

    def __init__(self, params: dict):
        super().__init__(params)
        self.docker_client = docker.from_env()
        base_name = self.params["experiment"]["docker"]["execute"]["name"]
        self.container_name = f"{base_name}_{self.params['jobid']}_{self.params['runid']}"
        if self.params["server_mode"]:
            self.container_name += "_server"
        self.log.info("[executor][docker] container_name=%s", self.container_name)

    def _deploy_job(self):
        """Pull image; remove any stale instance from a previous failed run."""
        image = self.params["experiment"]["docker"]["image"]
        self.log.info("[executor][docker][deploy] pulling image=%s", image)
        try:
            self.docker_client.images.pull(image)
            self.log.info("[executor][docker][deploy] image pull complete: %s", image)
        except Exception:
            self.log.exception("[executor][docker][deploy] image pull FAILED for %s", image)
            raise

        # Remove stale container from a previous run with same name
        try:
            stale = self.docker_client.containers.get(self.container_name)
            self.log.warning("[executor][docker][deploy] stale container found name=%s status=%s "
                             "— stopping and removing", self.container_name, stale.status)
            stale.stop()
            stale.remove()
            self.log.info("[executor][docker][deploy] stale container removed name=%s",
                          self.container_name)
        except docker.errors.NotFound:
            self.log.info("[executor][docker][deploy] no stale container found name=%s",
                          self.container_name)
        except Exception:
            self.log.exception("[executor][docker][deploy] error cleaning up stale container "
                               "name=%s", self.container_name)

    def _execute_job(self):
        """Launch container in background thread; enforce TTL; stream logs."""
        runid = self.client_params["runid"]
        jobid = self.client_params["jobid"]

        # BUG FIX: server_mode determines which TTL to enforce
        if self.params["server_mode"]:
            ttl_secs = self.params["ttl_secs"]
            self.log.info("[executor][docker][execute] SERVER MODE runid=%s ttl_secs=%d",
                          runid, ttl_secs)
        else:
            ttl_secs = self.params["length_secs"]
            self.log.info("[executor][docker][execute] CLIENT MODE runid=%s length_secs=%d",
                          runid, ttl_secs)

        # Start the container in a background thread (it blocks on log streaming)
        thread = threading.Thread(
            target=self._execute_job_loop,
            name=self.container_name,
            daemon=True)
        thread.start()
        self.log.info("[executor][docker][execute] container thread started name=%s",
                      self.container_name)

        # --- Wait for the container to reach 'running' state ---
        time_start = time_now()
        delta = 0
        container_started = False
        while delta <= ttl_secs:
            try:
                container = self.docker_client.containers.get(self.container_name)
                if container.status == "running":
                    container_started = True
                    if self.params["server_mode"]:
                        self.log.info("[executor][docker][execute] container running in SERVER mode "
                                      "— updating task TASK_COMPLETE taskid=%s",
                                      self.params["taskid"])
                        self.params["client"].update_task(
                            taskid=self.params["taskid"], status="TASK_COMPLETE")
                    else:
                        self.log.info("[executor][docker][execute] container is running "
                                      "runid=%s elapsed=%ds", runid, delta)
                    break
                else:
                    self.log.debug("[executor][docker][execute] waiting for container to start "
                                   "name=%s status=%s elapsed=%ds",
                                   self.container_name, container.status, delta)
            except docker.errors.NotFound:
                self.log.debug("[executor][docker][execute] container not yet visible name=%s "
                               "elapsed=%ds", self.container_name, delta)
            except Exception:
                self.log.exception("[executor][docker][execute] unexpected error polling "
                                   "container start name=%s", self.container_name)
                break

            # BUG FIX: thread.is_alive is a method, not a boolean attribute
            if not thread.is_alive():
                self.log.warning("[executor][docker][execute] container thread exited before "
                                 "container reached 'running' state name=%s elapsed=%ds",
                                 self.container_name, delta)
                break

            sleep(2)
            delta = int((time_now() - time_start).total_seconds())

        if not container_started:
            self.log.error("[executor][docker][execute] container failed to start within %ds "
                           "runid=%s name=%s", ttl_secs, runid, self.container_name)

        # Subtract start-up latency from the remaining TTL so we honour strict boundaries
        remaining_ttl = max(0, ttl_secs - delta)
        self.log.info("[executor][docker][execute] startup_delta=%ds remaining_ttl=%ds runid=%s",
                      delta, remaining_ttl, runid)

        # --- Enforce TTL: wait for container to exit or kill it ---
        time_start = time_now()
        delta = 0
        last_log_tick = -1
        while _container_is_running(self.docker_client, self.container_name) and delta <= remaining_ttl:
            sleep(1)
            delta = int((time_now() - time_start).total_seconds())
            tick = delta // 10
            if tick != last_log_tick:
                self.log.info("[executor][docker][execute] container running: "
                              "elapsed=%ds remaining_ttl=%ds runid=%s name=%s",
                              delta, remaining_ttl, runid, self.container_name)
                last_log_tick = tick

        if _container_is_running(self.docker_client, self.container_name):
            self.log.warning("[executor][docker][execute] TTL exceeded (%ds) — stopping container "
                             "runid=%s name=%s", remaining_ttl, runid, self.container_name)
            try:
                container = self.docker_client.containers.get(self.container_name)
                container.stop()
                self.log.info("[executor][docker][execute] container stopped by TTL runid=%s "
                              "name=%s", runid, self.container_name)
            except docker.errors.NotFound:
                self.log.info("[executor][docker][execute] container already gone when we tried "
                              "to stop it name=%s", self.container_name)
            except Exception:
                self.log.exception("[executor][docker][execute] error stopping container "
                                   "name=%s", self.container_name)
        else:
            self.log.info("[executor][docker][execute] container exited cleanly runid=%s "
                          "name=%s elapsed=%ds", runid, self.container_name, delta)

        thread.join(timeout=30)
        if thread.is_alive():
            self.log.warning("[executor][docker][execute] log-streaming thread still alive "
                             "after 30s join name=%s", self.container_name)

    def _execute_job_loop(self):
        """Launch container (blocking) and stream its stdout/stderr to the executor log."""
        runid = self.client_params["runid"]
        jobid = self.client_params["jobid"]

        src_path = os.path.join(
            self.params["executor"]["docker"]["execute"]["volume"]["source"],
            self.params["expdir"])
        dst_path = self.params["executor"]["docker"]["execute"]["volume"]["dest"]

        network_mode = None
        network = None
        if "network" in self.params["executor"]["docker"]["execute"]:
            network = self.params["executor"]["docker"]["execute"]["network"]
        else:
            network_mode = "host"

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

        shared = {src_path: {"bind": dst_path, "mode": "rw"}}

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
            "leotest": "true",
        }

        self.log.info("[executor][docker][run] launching container name=%s image=%s "
                      "network=%s network_mode=%s volume_src=%s volume_dst=%s "
                      "env=%s runid=%s jobid=%s",
                      self.container_name, image, network, network_mode,
                      src_path, dst_path, environment, runid, jobid)

        try:
            output = self.docker_client.containers.run(
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
                stdout=True,
                stderr=True,
                stream=True,
                environment=environment,
                labels=labels,
            )
            self.log.info("[executor][docker][run] container started — streaming logs "
                          "name=%s runid=%s", self.container_name, runid)
        except Exception:
            self.log.exception("[executor][docker][run] containers.run() raised name=%s "
                               "runid=%s — falling back to container.logs()", self.container_name, runid)
            try:
                container = self.docker_client.containers.get(self.container_name)
                output = container.logs(stdout=True, stderr=True, stream=True)
            except Exception:
                self.log.exception("[executor][docker][run] ALSO failed to get container logs "
                                   "name=%s runid=%s — giving up", self.container_name, runid)
                return

        self.log.info("[executor][docker][run] === LOG STREAM START name=%s ===",
                      self.container_name)
        try:
            for line in output:
                self.log.info("[container|%s] %s", self.container_name,
                              line.decode("utf-8", errors="replace").rstrip())
        except StopIteration:
            pass
        except Exception:
            self.log.exception("[executor][docker][run] error reading container log stream "
                               "name=%s runid=%s", self.container_name, runid)
        self.log.info("[executor][docker][run] === LOG STREAM END name=%s ===",
                      self.container_name)

    def _finish_job(self):
        """Stop and remove the experiment container."""
        try:
            container = self.docker_client.containers.get(self.container_name)
            self.log.info("[executor][docker][finish] stopping container name=%s status=%s",
                          self.container_name, container.status)
            container.stop()
            container.remove()
            self.log.info("[executor][docker][finish] container stopped and removed name=%s",
                          self.container_name)
        except docker.errors.NotFound:
            self.log.info("[executor][docker][finish] container already gone name=%s",
                          self.container_name)
        except Exception:
            self.log.exception("[executor][docker][finish] error removing container name=%s",
                               self.container_name)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    # Minimal bootstrap logging until we open the per-run log file
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s",
        stream=sys.stdout)

    log_bootstrap = logging.getLogger(__name__)
    log_bootstrap.info("[executor] ===== LEOScope Executor Starting =====")
    cfg.log_config_summary()

    parser = argparse.ArgumentParser(description="LEOScope experiment executor.")

    # server-mode args
    parser.add_argument("--taskid", metavar="TASKID", type=str, required=False)
    parser.add_argument("--runid",  metavar="RUNID",  type=str, required=False)
    parser.add_argument("--server", action="store_true")
    parser.add_argument("--no-server", dest="server", action="store_false")
    parser.set_defaults(server=False)
    parser.add_argument("--ttl-secs", metavar="TTL", type=int, default=300)

    # common args
    parser.add_argument("--setup-server",    action="store_true")
    parser.add_argument("--no-setup-server", dest="setup_server", action="store_false")
    parser.set_defaults(setup_server=False)
    parser.add_argument("--server-node",   metavar="NODEID", type=str, required=False)
    parser.add_argument("--length-secs",   metavar="LENGTH", type=int)
    parser.add_argument("--jobid",         metavar="JOBID",  type=str)
    parser.add_argument("--userid",        metavar="USERID", type=str)
    parser.add_argument("--nodeid",        metavar="NODEID", type=str)
    parser.add_argument("--access-token",  metavar="TOKEN",  type=str)
    parser.add_argument("--start-date",    metavar="START_DATE", type=str)
    parser.add_argument("--end-date",      metavar="END_DATE",   type=str)
    parser.add_argument("--resched-buffer", metavar="SECONDS", type=int,
                        default=cfg.SCHEDULER_RESCHED_BUFFER_SECS)
    parser.add_argument("--grpc-hostname", metavar="HOST", type=str,
                        default=cfg.GRPC_HOSTNAME)
    parser.add_argument("--grpc-port",     metavar="PORT", type=int,
                        default=cfg.GRPC_PORT)
    parser.add_argument("--mode",  metavar="MODE",     type=str, choices=["docker"])
    parser.add_argument("--type",  metavar="JOB_TYPE", type=str, choices=["cron", "atq"])
    overhead_grp = parser.add_mutually_exclusive_group(required=False)
    overhead_grp.add_argument("--overhead",    dest="overhead", action="store_true")
    overhead_grp.add_argument("--no-overhead", dest="overhead", action="store_false")
    parser.set_defaults(overhead=True)
    parser.add_argument("--workdir",         type=str, default=cfg.ARTIFACTDIR)
    parser.add_argument("--executor-config", type=str, default=cfg.EXECUTOR_CONFIG)

    args = parser.parse_args()

    taskid       = None
    runid        = None
    jobid        = args.jobid
    nodeid       = args.nodeid
    userid       = args.userid
    job_type     = args.type
    overhead     = args.overhead
    start_date   = args.start_date
    end_date     = args.end_date
    access_token = args.access_token

    log_bootstrap.info("[executor] args: jobid=%s nodeid=%s userid=%s type=%s "
                       "overhead=%s server_mode=%s setup_server=%s server_node=%s "
                       "length_secs=%s ttl_secs=%s grpc=%s:%d",
                       jobid, nodeid, userid, job_type, overhead, args.server,
                       args.setup_server, args.server_node,
                       args.length_secs, args.ttl_secs, args.grpc_hostname, args.grpc_port)

    # ------------------------------------------------------------------
    # gRPC client
    # ------------------------------------------------------------------
    log_bootstrap.info("[executor] connecting to orchestrator grpc=%s:%d nodeid=%s",
                       args.grpc_hostname, args.grpc_port, nodeid)
    client = LeotestClient(
        grpc_hostname=args.grpc_hostname,
        grpc_port=args.grpc_port,
        userid=nodeid,
        access_token=access_token)

    # ------------------------------------------------------------------
    # Global config (Azure fallback if experiment YAML has no cloud_config)
    # ------------------------------------------------------------------
    log_bootstrap.info("[executor] fetching global config from orchestrator")
    try:
        res = client.get_config()
        global_config = MessageToDict(res).get("config", {})
        blob_cfg = global_config.get("datastore", {}).get("blob", {})
        connection_string_global = blob_cfg.get("connectionString", "")
        container_global = blob_cfg.get("container", "")
        artifact_path_global = blob_cfg.get("artifactPath", cfg.AZURE_ARTIFACT_PATH)
        log_bootstrap.info("[executor] global config fetched: container=%s artifact_path=%s",
                           container_global, artifact_path_global)
    except Exception:
        log_bootstrap.exception("[executor] FAILED to fetch global config — using env defaults")
        connection_string_global = cfg.AZURE_CONNECTION_STRING
        container_global = cfg.AZURE_CONTAINER
        artifact_path_global = cfg.AZURE_ARTIFACT_PATH

    # ------------------------------------------------------------------
    # Server IP (needed before run ID is known)
    # ------------------------------------------------------------------
    server_ip = None
    if args.setup_server and args.server_node:
        log_bootstrap.info("[executor] fetching public_ip for server_node=%s", args.server_node)
        try:
            res = client.get_nodes(nodeid=args.server_node)
            msg = MessageToDict(res).get("nodes", [])
            if msg:
                server_ip = msg[0].get("publicIp")
                log_bootstrap.info("[executor] server_node=%s public_ip=%s",
                                   args.server_node, server_ip)
            else:
                log_bootstrap.warning("[executor] server_node=%s not found in node registry",
                                      args.server_node)
        except Exception:
            log_bootstrap.exception("[executor] FAILED to fetch server node IP server_node=%s",
                                    args.server_node)

    # ------------------------------------------------------------------
    # Scavenger mode check
    # ------------------------------------------------------------------
    log_bootstrap.info("[executor] checking scavenger mode for nodeid=%s", nodeid)
    terminate = {"flag": False, "status": "None", "reason": "None"}
    try:
        res = client.get_scavenger_status(nodeid)
        msg = MessageToDict(res)
        log_bootstrap.info("[executor] scavenger_status response: %s", msg)

        if msg.get("found"):
            scavenger_active = msg.get("scavengerModeActive", False)
            log_bootstrap.info("[executor] scavenger_mode_active=%s overhead=%s job_type=%s",
                               scavenger_active, overhead, job_type)

            if scavenger_active and overhead:
                log_bootstrap.warning("[executor] SCAVENGER MODE is active and job is overhead "
                                      "— attempting to terminate/reschedule jobid=%s", jobid)
                terminate["flag"] = True
                terminate["status"] = "TERMINATED"
                terminate["reason"] = "job run terminated: scavenger mode is active."

                if job_type == "atq":
                    starttime = time_now() + timedelta(seconds=int(args.resched_buffer))
                    log_bootstrap.info("[executor] rescheduling ATQ job jobid=%s "
                                       "new_start=%s end_date=%s", jobid, starttime, end_date)
                    try:
                        ret = client.reschedule_job_nearest(jobid, str(starttime), str(end_date))
                        ret_msg = MessageToDict(ret)
                        if ret_msg.get("rescheduled"):
                            log_bootstrap.info("[executor] ATQ job rescheduled jobid=%s "
                                               "message=%s", jobid, ret_msg.get("message"))
                            terminate["status"] = "RESCHEDULED"
                            terminate["reason"] = (
                                "job rescheduled (scavenger active): %s" % ret_msg.get("message"))
                        else:
                            # BUG FIX: reschedule failure must still keep terminate['flag']=True
                            # so the executor does NOT proceed with execution.
                            log_bootstrap.error("[executor] ATQ job reschedule FAILED jobid=%s "
                                                "message=%s — marking RESCHEDULE_FAILED",
                                                jobid, ret_msg.get("message"))
                            terminate["flag"] = True
                            terminate["status"] = "RESCHEDULE_FAILED"
                            terminate["reason"] = (
                                "reschedule failed (scavenger active): %s" % ret_msg.get("message"))
                    except Exception:
                        log_bootstrap.exception("[executor] exception during ATQ reschedule "
                                                "jobid=%s — aborting run", jobid)
                        terminate["flag"] = True
                        terminate["status"] = "RESCHEDULE_FAILED"
                        terminate["reason"] = "reschedule raised exception (scavenger active)"
            else:
                log_bootstrap.info("[executor] scavenger mode not active or job is non-overhead "
                                   "— proceeding with execution")
        else:
            log_bootstrap.warning("[executor] nodeid=%s not found while fetching scavenger status "
                                  "— proceeding with execution", nodeid)
    except Exception:
        log_bootstrap.exception("[executor] FAILED to check scavenger status for nodeid=%s "
                                "— proceeding with execution", nodeid)

    # ------------------------------------------------------------------
    # Run ID and path construction
    # ------------------------------------------------------------------
    if not args.server:
        timenow = time_now()
        runid   = uuid.uuid4().hex
        log_bootstrap.info("[executor] generated new runid=%s timenow=%s", runid, timenow)
    else:
        taskid       = args.taskid
        runid        = args.runid
        runid_server = "%s_server" % args.runid
        log_bootstrap.info("[executor] SERVER mode: taskid=%s runid=%s runid_server=%s",
                           taskid, runid, runid_server)
        try:
            res = MessageToDict(client.get_runs(runid=runid))
            if "runs" in res and res["runs"]:
                timenow = datetimeParse(res["runs"][0]["startTime"])
                nodeid  = res["runs"][0]["nodeid"]
                userid  = res["runs"][0]["userid"]
                log_bootstrap.info("[executor] resolved from existing run: nodeid=%s userid=%s "
                                   "timenow=%s", nodeid, userid, timenow)
            else:
                log_bootstrap.warning("[executor] run record not found for runid=%s — "
                                      "using current time", runid)
                timenow = time_now()
        except Exception:
            log_bootstrap.exception("[executor] FAILED to fetch run record for runid=%s — "
                                    "using current time", runid)
            timenow = time_now()

    runid_server = "%s_server" % runid if args.server else None

    # ------------------------------------------------------------------
    # Memcached session marker — prevents duplicate executor invocations
    # ------------------------------------------------------------------
    memcached_addr = "%s:%d" % (cfg.MEMCACHED_HOST, cfg.MEMCACHED_PORT)
    log_bootstrap.info("[executor] connecting to memcached at %s", memcached_addr)
    try:
        sessionstore = memcache_client((cfg.MEMCACHED_HOST, cfg.MEMCACHED_PORT))
        session_key = "%s_executor" % runid
        sessionstore.set(key=session_key, value="1", expire=int(args.length_secs))
        log_bootstrap.info("[executor] memcached session set key=%s expire=%ds",
                           session_key, args.length_secs)
    except Exception:
        log_bootstrap.exception("[executor] FAILED to set memcached session — "
                                "proceeding without session guard")
        sessionstore = None

    # ------------------------------------------------------------------
    # Directory layout
    # ------------------------------------------------------------------
    expdir = os.path.join(nodeid, jobid,
                          str(timenow.year), str(timenow.month), str(timenow.day), runid)
    expdir_upload = os.path.join(nodeid, jobid,
                                 str(timenow.year), str(timenow.month), str(timenow.day))
    remote_path     = os.path.join(artifact_path_global, expdir_upload)
    remote_path_job = os.path.join(artifact_path_global, nodeid, jobid)
    workdir = os.path.join(args.workdir, expdir)

    log_bootstrap.info("[executor] expdir=%s workdir=%s remote_path=%s",
                       expdir, workdir, remote_path)

    if not os.path.exists(workdir):
        os.makedirs(workdir)
        log_bootstrap.info("[executor] created workdir=%s", workdir)

    # ------------------------------------------------------------------
    # Per-run log files (executor stdout/stderr captured inside workdir)
    # ------------------------------------------------------------------
    logname = "executor_server" if args.server else "executor"
    log_stdout_path = os.path.join(workdir, logname + ".stdout")
    log_stderr_path = os.path.join(workdir, logname + ".stderr")

    executor_log_stdout = open(log_stdout_path, "w")
    executor_log_stderr = open(log_stderr_path, "w")
    sys.stdout = executor_log_stdout
    sys.stderr = executor_log_stderr

    fileh = logging.StreamHandler(stream=executor_log_stdout)
    formatter = logging.Formatter(
        "%(asctime)s %(filename)s:%(lineno)s %(thread)d %(levelname)s %(message)s")
    fileh.setFormatter(formatter)
    root_log = logging.getLogger()
    for hdlr in root_log.handlers[:]:
        root_log.removeHandler(hdlr)
    root_log.addHandler(fileh)

    log_run = logging.getLogger(__name__)
    log_run.info("[executor] ===== RUN LOG STARTED =====")
    log_run.info("[executor] runid=%s jobid=%s nodeid=%s userid=%s job_type=%s overhead=%s",
                 runid, jobid, nodeid, userid, job_type, overhead)
    log_run.info("[executor] workdir=%s log_stdout=%s log_stderr=%s",
                 workdir, log_stdout_path, log_stderr_path)
    log_run.info("[executor] remote_path=%s", remote_path)

    # ------------------------------------------------------------------
    # Experiment and executor config
    # ------------------------------------------------------------------
    experiment_config_remote = os.path.join(remote_path_job, "experiment-config.yaml")
    experiment_args_remote   = os.path.join(remote_path_job, "experiment-args.json")
    experiment_config_dst    = os.path.join(workdir, "experiment-config.yaml")
    executor_config_dst      = os.path.join(workdir, "executor-config.yaml")
    experiment_args_dst      = os.path.join(workdir, "experiment-args.json")

    azclient = StorageDirectoryClient(connection_string_global, container_global)
    exp_args_from_orch = None

    # --- experiment-config.yaml ---
    if azclient.check_blob_exists(experiment_config_remote):
        log_run.info("[executor][config] downloading experiment-config from blob: "
                     "container=%s blob=%s → %s",
                     container_global, experiment_config_remote, experiment_config_dst)
        download_file(connection_string_global, container_global,
                      experiment_config_remote, experiment_config_dst)
    else:
        log_run.info("[executor][config] experiment-config not in blob — "
                     "fetching from orchestrator for jobid=%s → %s", jobid, experiment_config_dst)
        try:
            res = client.get_job_by_id(jobid)
            exp_args_from_orch = MessageToDict(res)
            exp_config_yaml = exp_args_from_orch.get("config", "")
            with open(experiment_config_dst, "w") as f:
                f.write(exp_config_yaml)
            log_run.info("[executor][config] experiment-config written to %s (%d bytes)",
                         experiment_config_dst, len(exp_config_yaml))
        except Exception:
            log_run.exception("[executor][config] FAILED to fetch experiment-config for jobid=%s",
                              jobid)
            raise

    # --- experiment-args.json ---
    if azclient.check_blob_exists(experiment_args_remote):
        log_run.info("[executor][config] downloading experiment-args from blob: "
                     "container=%s blob=%s → %s",
                     container_global, experiment_args_remote, experiment_args_dst)
        download_file(connection_string_global, container_global,
                      experiment_args_remote, experiment_args_dst)
    else:
        log_run.info("[executor][config] experiment-args not in blob — "
                     "fetching from orchestrator for jobid=%s → %s", jobid, experiment_args_dst)
        if not exp_args_from_orch:
            res = client.get_job_by_id(jobid)
            exp_args_from_orch = MessageToDict(res)

        # BUG FIX: 'config' key check should be on exp_args_from_orch (dict), not
        # exp_config_yaml (string). Previously: `if 'config' in exp_config:` where
        # exp_config was the YAML string — always True since it searched substrings.
        args_to_dump = {k: v for k, v in exp_args_from_orch.items() if k != "config"}
        with open(experiment_args_dst, "w") as f:
            json.dump(args_to_dump, f, indent=2)
        log_run.info("[executor][config] experiment-args written to %s", experiment_args_dst)

    # --- executor-config.yaml ---
    log_run.info("[executor][config] copying executor-config: %s → %s",
                 args.executor_config, executor_config_dst)
    shutil.copy(args.executor_config, executor_config_dst)

    # --- parse configs ---
    log_run.info("[executor][config] parsing experiment-config.yaml from %s",
                 experiment_config_dst)
    with open(experiment_config_dst, "r") as f:
        try:
            experiment_config_dict = yaml.safe_load(f)
        except yaml.YAMLError:
            log_run.exception("[executor][config] FAILED to parse experiment-config.yaml "
                              "path=%s", experiment_config_dst)
            raise
    log_run.info("[executor][config] experiment-config sections: %s",
                 list(experiment_config_dict.keys()) if experiment_config_dict else "EMPTY")

    log_run.info("[executor][config] parsing executor-config.yaml from %s", executor_config_dst)
    with open(executor_config_dst, "r") as f:
        try:
            executor_config_dict = yaml.safe_load(f)
        except yaml.YAMLError:
            log_run.exception("[executor][config] FAILED to parse executor-config.yaml "
                              "path=%s", executor_config_dst)
            raise
    log_run.info("[executor][config] executor-config sections: %s",
                 list(executor_config_dict.keys()) if executor_config_dict else "EMPTY")

    # Experiment-specific Azure credentials override the global fallback
    cloud_cfg = experiment_config_dict.get("cloud_config", {})
    connection_string = cloud_cfg.get("connection_string", connection_string_global)
    container        = cloud_cfg.get("container", container_global)
    log_run.info("[executor][config] azure: container=%s (source=%s)",
                 container, "experiment-config" if cloud_cfg else "global-config")

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------
    params = {
        "server_mode":  args.server,
        "setup_server": args.setup_server,
        "server_node":  args.server_node,
        "server_ip":    server_ip,
        "ttl_secs":     args.ttl_secs,
        "start_date":   start_date,
        "end_date":     end_date,
        "length_secs":  args.length_secs,
        "taskid":       taskid,
        "runid":        runid,
        "runid_server": runid_server,
        "nodeid":       nodeid,
        "jobid":        jobid,
        "userid":       userid,
        "connection_string": connection_string,
        "container":    container,
        "remote_path":  remote_path,
        "workdir":      workdir,
        "expdir":       expdir,
        "experiment":   experiment_config_dict,
        "executor":     executor_config_dict,
        "client":       client,
        "start_time":   str(timenow),
        "log":          log_run,
        "executor_log_stdout": executor_log_stdout,
        "executor_log_stderr": executor_log_stderr,
        "terminate":    terminate,
        "overhead":     overhead,
        "job_type":     job_type,
        "sessionstore": sessionstore,
    }

    log_run.info("[executor] building executor mode=%s", args.mode)
    if args.mode == "docker":
        executor = LeotestExecutorDocker(params=params)
    else:
        raise ValueError("Unsupported executor mode: %s" % args.mode)

    try:
        executor.run()
    except Exception:
        log_run.exception("[executor] UNHANDLED exception in executor.run() "
                          "runid=%s jobid=%s nodeid=%s", runid, jobid, nodeid)
        raise
    finally:
        # Clean up memcached session marker regardless of success/failure
        if sessionstore:
            try:
                sessionstore.delete(key="%s_executor" % runid)
                log_run.info("[executor] memcached session deleted key=%s_executor", runid)
            except Exception:
                log_run.warning("[executor] could not delete memcached session key=%s_executor",
                                runid)

    log_run.info("[executor] ===== RUN LOG ENDED runid=%s jobid=%s =====", runid, jobid)


if __name__ == "__main__":
    main()
