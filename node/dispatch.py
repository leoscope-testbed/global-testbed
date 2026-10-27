"""Scheduler-owned dispatch of one-shot (ATQ) experiment runs.

Why this exists instead of ``at``/atd
-------------------------------------
One-shot jobs used to be handed to the Unix ``at`` queue. That lost runs
silently, in several independent ways:

* ``python-atd`` submits ``at`` with ``inherit_env = False``, so the script atd
  later runs carries nothing but ``PATH=/bin:/usr/bin``.
* atd mails a job's stderr. The node container has no MTA, so every traceback
  and every argparse error the executor wrote was discarded. A job that failed
  on its first line looked exactly like a job that never started.
* ``at`` stores fire times at minute resolution, so a start time with seconds
  was silently rounded down.
* ``atd.at`` refuses a time in the past, so a job whose start time passed before
  it reached the queue -- a node restart, a slow orchestrator, a stopped atd --
  could never run again, and nothing anywhere recorded why.

The scheduler already polls the orchestrator every few seconds, so it fires
one-shot runs itself. Dispatching from here keeps the executor's stdout *and*
stderr in a file we own, keeps the exit code, and -- through :meth:`verify` --
lets the node tell the orchestrator when a run could not be started at all,
instead of leaving the reconciler to guess from silence.

State
-----
Each scheduler tick runs in a fresh forked process, so nothing can be
remembered in memory. One occurrence is identified by ``jobid`` and its fire
time, and what the node did about it is recorded in a JSON file under the
artifact directory -- a host-mounted volume, so the record survives a container
restart and a job is never dispatched twice.
"""

import json
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timedelta

from common import config as cfg
from common import job_events as je
from common import run_report

log = logging.getLogger(__name__)

STATE_VERSION = 1

# Outcomes recorded per occurrence. RESERVED is written just before the executor
# is started, so an occurrence is never dispatched twice; it only survives a
# crash in that instant, and is then treated as a run that never reported.
OUTCOME_RESERVED = "RESERVED"
OUTCOME_DISPATCHED = "DISPATCHED"
OUTCOME_TOO_LATE = "TOO_LATE"
OUTCOME_SPAWN_FAILED = "SPAWN_FAILED"

# A dispatch record is kept this long after its deadline so that a repeated
# verification cannot report the same occurrence twice, then dropped so the
# state file stays small.
RECORD_RETENTION_SECS = 7 * 24 * 3600

# A run is attributed to an occurrence when it starts no earlier than this
# before the fire time. Matches orchestrator/reconciler.MATCH_SKEW_SECS.
MATCH_SKEW_SECS = 5


def _compact(occurrence):
    return occurrence.strftime("%Y%m%dT%H%M%S")


class LeotestAtqDispatcher:
    """Fires one-shot experiment runs, and reports the ones that could not fire.

    :param spawn: injection point for tests; called as
        ``spawn(argv, cwd, log_path)`` and returns the child's pid.
    :param clock: callable returning the current naive UTC datetime.
    """

    def __init__(self, python_bin, module_name, workdir, nodeid,
                 artifactdir="/artifacts/",
                 grpc_hostname="localhost",
                 grpc_port=50051,
                 executor_config="/executor-config.yaml",
                 access_token="",
                 resched_buffer_secs=None,
                 max_lateness_secs=None,
                 grace_secs=None,
                 state_dir=None,
                 spawn=None,
                 clock=None):
        self.python_bin = python_bin
        self.module_name = module_name
        self.workdir = workdir
        self.nodeid = nodeid
        self.artifactdir = artifactdir
        self.grpc_hostname = grpc_hostname
        self.grpc_port = grpc_port
        self.executor_config = executor_config
        self.access_token = access_token
        self.resched_buffer_secs = (cfg.SCHEDULER_RESCHED_BUFFER_SECS
                                    if resched_buffer_secs is None else resched_buffer_secs)
        self.max_lateness_secs = (cfg.SCHEDULER_MAX_DISPATCH_LATENESS_SECS
                                  if max_lateness_secs is None else max_lateness_secs)
        self.grace_secs = cfg.JOB_MISSED_GRACE_SECS if grace_secs is None else grace_secs
        self.state_dir = state_dir or os.path.join(artifactdir, ".leoscope")
        self.state_path = os.path.join(self.state_dir, "atq_dispatch.json")
        self._spawn = spawn or self._spawn_detached
        self._clock = clock or datetime.utcnow

    # ------------------------------------------------------------------
    # state file
    # ------------------------------------------------------------------
    def _load(self):
        try:
            with open(self.state_path, "r") as handle:
                state = json.load(handle)
        except FileNotFoundError:
            return {"version": STATE_VERSION, "dispatches": {}}
        except (OSError, ValueError):
            # A truncated or unreadable file must not stop the node from running
            # jobs. Starting over can re-dispatch an occurrence whose record was
            # lost, which is the safe direction: a duplicate run is visible, a
            # silently skipped one is not.
            log.exception("[dispatch] state file %s is unreadable; starting a new one",
                          self.state_path)
            return {"version": STATE_VERSION, "dispatches": {}}
        if not isinstance(state, dict) or not isinstance(state.get("dispatches"), dict):
            log.error("[dispatch] state file %s has an unexpected shape; starting a new one",
                      self.state_path)
            return {"version": STATE_VERSION, "dispatches": {}}
        return state

    def _save(self, state):
        """Replace the state file atomically. Returns whether it was written.

        Atomically, so a tick killed mid-write cannot leave a half-written
        record behind. The return value matters: dispatching without being able
        to record it would start the same experiment again on every tick.
        """
        try:
            os.makedirs(self.state_dir, exist_ok=True)
            handle = tempfile.NamedTemporaryFile(
                "w", dir=self.state_dir, prefix="atq_dispatch.", suffix=".tmp", delete=False)
        except Exception:
            log.exception("[dispatch] could not open a new state file in %s", self.state_dir)
            return False
        try:
            with handle:
                json.dump(state, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(handle.name, self.state_path)
            return True
        except Exception:
            log.exception("[dispatch] could not write state file %s", self.state_path)
            try:
                os.unlink(handle.name)
            except OSError:
                pass
            return False

    # ------------------------------------------------------------------
    # dispatch
    # ------------------------------------------------------------------
    @staticmethod
    def record_key(jobid, occurrence):
        return "%s|%s" % (jobid, occurrence.isoformat())

    def _deadline(self, occurrence, end_date, length_secs):
        """When this occurrence stops being allowed to report.

        The whole scheduled window counts, not just the experiment's own
        duration: the orchestrator may move a one-shot job anywhere inside
        ``[start_date, end_date]``, so a run that starts late is still this
        occurrence's run. Kept in step with
        ``orchestrator.reconciler.JobReconciler._due_offset``.
        """
        scheduled_end = max(occurrence + timedelta(seconds=int(length_secs or 0)), end_date)
        return scheduled_end + timedelta(seconds=self.grace_secs)

    def dispatch_due(self, jobs):
        """Start an executor for every one-shot job whose fire time has arrived.

        Returns the list of occurrence keys dispatched by this call.
        """
        now = self._clock()
        state = self._load()
        records = state["dispatches"]
        dispatched = []

        for job in jobs:
            jobid = job.get_jobid()
            try:
                occurrence = je.parse_utc(job.start_date)
                end_date = je.parse_utc(job.end_date)
            except Exception:
                log.exception("[dispatch] jobid=%s has unreadable dates "
                              "(start=%r end=%r); cannot dispatch it",
                              jobid, job.start_date, job.end_date)
                continue

            key = self.record_key(jobid, occurrence)
            if key in records:
                log.debug("[dispatch] occurrence already handled key=%s outcome=%s",
                          key, records[key].get("outcome"))
                continue

            if now < occurrence:
                log.debug("[dispatch] jobid=%s not due yet (fires %s, now %s)",
                          jobid, occurrence.isoformat(), now.isoformat())
                continue

            lateness = int((now - occurrence).total_seconds())
            record = {
                "jobid": jobid,
                "nodeid": job.nodeid or self.nodeid,
                "userid": job.userid,
                "occurrence": occurrence.isoformat(),
                "end_date": end_date.isoformat(),
                "length_secs": int(job.get_length_secs() or 0),
                "deadline": self._deadline(occurrence, end_date,
                                           job.get_length_secs()).isoformat(),
                "lateness_secs": lateness,
                "verified": False,
            }

            if lateness > self.max_lateness_secs:
                # Nothing is gained by starting an experiment this far past its
                # slot, but the person who scheduled it has to be told why it
                # did not run, which the at queue never did.
                log.error("[dispatch] NOT dispatching jobid=%s: fire time %s passed %ds ago "
                          "(limit %ds)", jobid, occurrence.isoformat(), lateness,
                          self.max_lateness_secs)
                record["outcome"] = OUTCOME_TOO_LATE
                record["error"] = (
                    "The node saw this occurrence %ds after its fire time %s, past the %ds "
                    "limit, so the experiment was not started. The node was most likely not "
                    "running, or could not reach the orchestrator, when the job was due."
                    % (lateness, occurrence.isoformat(), self.max_lateness_secs))
                records[key] = record
                continue

            argv = self._build_argv(job)
            log_path = self._log_path(jobid, occurrence)
            record["log_path"] = log_path
            record["outcome"] = OUTCOME_RESERVED

            # Recorded before the process starts, not after. If this node dies
            # in between, the occurrence is reported as never having run, which
            # is true and visible. The other order risks a state file that
            # cannot be written sending the same experiment off again on every
            # tick -- worse than the silence this module replaced.
            records[key] = record
            if not self._save(state):
                log.error("[dispatch] cannot record a dispatch in %s, so jobid=%s is NOT "
                          "being started: a dispatch that is not recorded would be "
                          "repeated on every tick", self.state_path, jobid)
                del records[key]
                continue

            log.info("[dispatch] starting executor jobid=%s occurrence=%s lateness=%ds "
                     "log=%s (path inside this container; on the host it is under "
                     "whatever is mounted at %s)",
                     jobid, occurrence.isoformat(), lateness, log_path, self.artifactdir)
            try:
                pid = self._spawn(argv, self.workdir, log_path)
            except Exception as exc:
                log.exception("[dispatch] FAILED to start the executor for jobid=%s", jobid)
                record["outcome"] = OUTCOME_SPAWN_FAILED
                record["error"] = "The node could not start the executor process: %s" % exc
            else:
                record["outcome"] = OUTCOME_DISPATCHED
                record["pid"] = pid
                record["dispatched_at"] = now.isoformat()
                dispatched.append(key)
                log.info("[dispatch] executor started jobid=%s pid=%s", jobid, pid)

            self._save(state)

        self._save(state)
        return dispatched

    def _log_path(self, jobid, occurrence):
        directory = os.path.join(self.artifactdir, "dispatch", jobid, _compact(occurrence))
        os.makedirs(directory, exist_ok=True)
        return os.path.join(directory, "executor.out")

    def _build_argv(self, job):
        """The executor's command line, as an argument list.

        Built as a list and spawned without a shell, so no value in it can be
        split, globbed or treated as a redirection. Only flags the executor
        declares are passed: the job's ``params`` (``mode``/``deploy``/
        ``execute``/``finish``) are not executor options, and passing them as
        ``--deploy=...`` made argparse exit 2 before the run began.
        """
        argv = [
            self.python_bin, "-m", self.module_name,
            "--jobid=%s" % job.get_jobid(),
            "--nodeid=%s" % self.nodeid,
            "--userid=%s" % job.userid,
            "--start-date=%s" % job.start_date,
            "--end-date=%s" % job.end_date,
            "--length-secs=%s" % (job.get_length_secs() or 0),
            "--resched-buffer=%d" % self.resched_buffer_secs,
            "--access-token=%s" % self.access_token,
            "--workdir=%s" % self.artifactdir,
            "--grpc-hostname=%s" % self.grpc_hostname,
            "--grpc-port=%s" % self.grpc_port,
            "--executor-config=%s" % self.executor_config,
            "--mode=docker",
            "--type=atq",
            "--overhead" if job.overhead else "--no-overhead",
        ]
        if job.server:
            argv += ["--setup-server", "--server-node=%s" % job.server]
        return argv

    @staticmethod
    def _spawn_detached(argv, cwd, log_path):
        """Start the executor in its own session with stdout and stderr captured.

        Its own session, because the tick that starts it is a short-lived forked
        process the scheduler kills after 60s; the run must outlive it. Both
        streams go to one file, so an early traceback -- which atd used to mail
        into the void -- lands somewhere the node can read back.
        """
        stream = open(log_path, "ab", buffering=0)
        try:
            process = subprocess.Popen(
                argv,
                cwd=cwd,
                stdin=subprocess.DEVNULL,
                stdout=stream,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                close_fds=True,
                env=os.environ.copy())
        finally:
            stream.close()
        return process.pid

    # ------------------------------------------------------------------
    # verification: no occurrence may end without a verdict
    # ------------------------------------------------------------------
    def verify(self, client):
        """Report occurrences whose deadline passed without an executor report.

        The executor reports its own failures once it is running. This closes
        the case it cannot report: it never started, or it died before its first
        gRPC call. For each such occurrence the node writes a FAILED run row
        carrying the dispatch log, so the failure is visible and downloadable
        rather than showing up as an unexplained MISSED.
        """
        now = self._clock()
        state = self._load()
        records = state["dispatches"]
        changed = False
        reported = []

        for key, record in sorted(records.items()):
            if record.get("verified"):
                if self._expired(record, now):
                    self._discard_log(record)
                    del records[key]
                    changed = True
                continue
            try:
                deadline = je.parse_utc(record["deadline"])
            except Exception:
                log.exception("[dispatch][verify] record %s has no readable deadline; dropping it",
                              key)
                del records[key]
                changed = True
                continue
            if now < deadline:
                continue

            if record.get("outcome") == OUTCOME_DISPATCHED and self._run_reported(client, record):
                log.info("[dispatch][verify] occurrence reported by the executor, nothing to do "
                         "key=%s", key)
            else:
                self._report_unreported(client, record)
                reported.append(key)
            record["verified"] = True
            changed = True

        if changed:
            self._save(state)
        return reported

    @staticmethod
    def _discard_log(record):
        """Remove a captured executor log once its occurrence is forgotten.

        Its contents are already in the uploaded report if they mattered, and
        the artifact volume is not a log archive.
        """
        log_path = record.get("log_path")
        if not log_path:
            return
        try:
            os.unlink(log_path)
            os.rmdir(os.path.dirname(log_path))
        except OSError:
            pass

    @staticmethod
    def _expired(record, now):
        try:
            deadline = je.parse_utc(record["deadline"])
        except Exception:
            return True
        return now - deadline > timedelta(seconds=RECORD_RETENTION_SECS)

    def _run_reported(self, client, record):
        """True when a run for this occurrence exists in the orchestrator.

        An error reaching the orchestrator counts as reported: the node must
        never overwrite a real run row just because it could not read it.
        """
        from google.protobuf.json_format import MessageToDict

        occurrence = je.parse_utc(record["occurrence"])
        deadline = je.parse_utc(record["deadline"])
        try:
            res = MessageToDict(client.get_runs(jobid=record["jobid"]))
        except Exception:
            log.exception("[dispatch][verify] could not read runs for jobid=%s; "
                          "assuming the executor reported", record["jobid"])
            return True
        for run in res.get("runs", []):
            start = run.get("startTime")
            if not start:
                continue
            try:
                started = je.parse_utc(start)
            except Exception:
                continue
            if (occurrence - timedelta(seconds=MATCH_SKEW_SECS)) <= started <= deadline:
                return True
        return False

    def _report_unreported(self, client, record):
        """Write the FAILED run row and job event for an occurrence that never ran."""
        jobid = record["jobid"]
        occurrence = je.parse_utc(record["occurrence"])
        outcome = record.get("outcome")
        # Deliberately the id the reconciler would use for this occurrence, so
        # the node's explained row and the reconciler's bare MISSED row are the
        # same row. Whichever lands first wins; the reconciler only inserts when
        # absent, and it treats this row as the occurrence's report.
        runid = je.occurrence_runid(jobid, occurrence)

        if outcome == OUTCOME_TOO_LATE:
            reason = je.REASON_DISPATCH_TOO_LATE
            headline = "the node did not start the experiment"
        elif outcome == OUTCOME_SPAWN_FAILED:
            reason = je.REASON_DISPATCH_FAILED
            headline = "the node could not start the executor process"
        else:
            reason = je.REASON_EXECUTOR_NO_REPORT
            headline = ("the executor was started but never reported a run "
                        "before the deadline")

        log_path = record.get("log_path")
        captured = run_report.tail(log_path) if log_path else "(the executor was never started)"
        detail = record.get("error", "")

        report = run_report.build_report(
            "LEOScope dispatch failure: %s" % jobid,
            [("Job", jobid),
             ("Node", record.get("nodeid")),
             ("User", record.get("userid")),
             ("Run", runid),
             ("Scheduled fire time (UTC)", record["occurrence"]),
             ("Scheduled window end (UTC)", record.get("end_date")),
             ("Experiment length (s)", record.get("length_secs")),
             ("Report deadline (UTC)", record.get("deadline")),
             ("Dispatched at (UTC)", record.get("dispatched_at")),
             ("Dispatch outcome", outcome),
             ("Started late by (s)", record.get("lateness_secs")),
             ("Reason code", reason),
             ("Executor pid", record.get("pid")),
             # The path as the node container sees it. On the host it is the
             # same path under whatever is mounted at the artifact directory.
             ("Executor log (in the node container)", log_path)],
            [("What happened", "This occurrence produced no run: %s.\n%s" % (headline, detail)),
             ("Executor stdout and stderr", captured)])

        blob_url = self._upload_report(client, record, runid, occurrence, report)
        message = je.truncate("%s. See the run report for the executor's output." % headline
                              if not detail else "%s: %s" % (headline, detail))

        try:
            client.update_run(
                runid=runid, jobid=jobid,
                nodeid=record.get("nodeid") or self.nodeid,
                userid=record.get("userid") or "",
                start_time=record["occurrence"],
                end_time=record.get("deadline") or record["occurrence"],
                status=je.STATUS_FAILED,
                status_message=message,
                blob_url=blob_url,
                stage=je.STAGE_DISPATCH,
                reason_code=reason)
            log.warning("[dispatch][verify] recorded FAILED run jobid=%s runid=%s reason=%s",
                        jobid, runid, reason)
        except Exception:
            log.exception("[dispatch][verify] could not write the FAILED run row jobid=%s", jobid)

        try:
            client.report_job_event(
                jobid=jobid, stage=je.STAGE_DISPATCH, outcome=je.OUTCOME_FAILED,
                reason_code=reason, message=message, runid=runid,
                nodeid=record.get("nodeid") or self.nodeid,
                userid=record.get("userid") or "",
                occurrence=record["occurrence"], source="node")
        except Exception:
            log.exception("[dispatch][verify] could not report the job event jobid=%s", jobid)

    def _upload_report(self, client, record, runid, occurrence, report):
        """Upload the report next to where this run's artifacts would have gone."""
        from google.protobuf.json_format import MessageToDict

        connection_string = cfg.AZURE_CONNECTION_STRING
        container = cfg.AZURE_CONTAINER
        artifact_path = cfg.AZURE_ARTIFACT_PATH
        try:
            blob_cfg = (MessageToDict(client.get_config())
                        .get("config", {}).get("datastore", {}).get("blob", {}))
            connection_string = blob_cfg.get("connectionString") or connection_string
            container = blob_cfg.get("container") or container
            artifact_path = blob_cfg.get("artifactPath") or artifact_path
        except Exception:
            log.exception("[dispatch][verify] could not read the global config; "
                          "falling back to this node's Azure settings for the report")

        remote_path = os.path.join(artifact_path, record.get("nodeid") or self.nodeid,
                                   record["jobid"], str(occurrence.year),
                                   str(occurrence.month), str(occurrence.day))
        return run_report.upload_text(connection_string, container,
                                      run_report.report_blob_path(remote_path, runid), report)
