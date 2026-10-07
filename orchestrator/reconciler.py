"""Records scheduled occurrences that no executor reported on as MISSED.

The orchestrator is the only component that sees both the schedule and the node
heartbeats, so "scheduled but never reported" is decided here. Each pass:

1. Refreshes a snapshot of every live cron/atq job into ``job_reconcile_state``.
   The snapshot exists because the jobs collection has a TTL index on
   ``expire_at``: a job disappears shortly after its end date, while its last
   occurrences may still be inside the grace period.
2. For each snapshot, finds the occurrences whose own scheduled duration plus
   the grace period have elapsed and have no run starting near them. Each such
   occurrence becomes a MISSED run row (shown in the runs tab) and a
   ``RECONCILE`` job event.

   The grace period is measured from the occurrence's scheduled *end*
   (fire time + the job's ``length_secs``), not from its fire time. A real
   deploy — config fetch, image pull, Azure/blob calls — can easily take
   longer than a short grace period on its own, well within a normal-length
   experiment, so judging from the fire time alone would report a merely slow
   but otherwise fine run as missed.

Every write uses a deterministic id, so overlapping or repeated passes cannot
create duplicates.
"""

import logging
import threading
from datetime import datetime, timedelta

from common import config as cfg
from common import job_events as je

log = logging.getLogger(__name__)

# The executor records its start time when cron or atd launches it, so a run
# starts at or just after its fire time. A run is attributed to the latest
# occurrence that began no later than the run, allowing only this much clock
# skew. A wide window would let one run cover several adjacent occurrences and
# hide the silent ones.
MATCH_SKEW_SECS = 5

# When a job vanishes from the jobs collection this close to its end date, the
# TTL index most likely removed it at its natural end. Judge through the end.
# Otherwise the job was deleted by a user, and only occurrences it existed for
# are judged.
NATURAL_EXPIRY_WINDOW_SECS = 120


def cron_string_from_schedule(schedule):
    if not isinstance(schedule, dict):
        return ""
    return "%s %s %s %s %s" % (
        schedule.get("minute", "*"),
        schedule.get("hour", "*"),
        schedule.get("day_of_month", "*"),
        schedule.get("month", "*"),
        schedule.get("day_of_week", "*"),
    )


def snapshot_from_job(doc):
    job_type = doc.get("type")
    return {
        "jobid": doc["id"],
        "type": job_type,
        "cron": cron_string_from_schedule(doc.get("schedule")) if job_type == "cron" else "",
        "start_date": doc.get("start_date"),
        "end_date": doc.get("end_date"),
        "nodeid": doc.get("nodeid"),
        "userid": doc.get("userid"),
        "created_at": doc.get("created_at"),
        "length_secs": doc.get("length_secs") or 0,
    }


class JobReconciler:
    """Judges scheduled occurrences against executor reports.

    :param db: datastore providing the methods used below
        (see :class:`orchestrator.datastore.LeotestDatastoreMongo`).
    :param clock: callable returning the current naive UTC datetime. Tests pass
        a fixed clock.
    """

    def __init__(self, db, grace_secs=None, stale_secs=None, lookback_secs=None,
                 interval_secs=None, clock=None):
        self.db = db
        self.grace_secs = cfg.JOB_MISSED_GRACE_SECS if grace_secs is None else grace_secs
        self.stale_secs = cfg.NODE_STALE_SECS if stale_secs is None else stale_secs
        self.lookback_secs = cfg.RECONCILER_LOOKBACK_SECS if lookback_secs is None else lookback_secs
        self.interval_secs = cfg.RECONCILER_INTERVAL_SECS if interval_secs is None else interval_secs
        self._clock = clock or datetime.utcnow
        self._stop = threading.Event()
        self._thread = None
        self._zone_warned = set()

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self):
        """Run passes in a daemon thread every ``interval_secs``."""
        log.info("[reconciler] starting: grace=%ds interval=%ds stale=%ds lookback=%ds",
                 self.grace_secs, self.interval_secs, self.stale_secs, self.lookback_secs)
        self._thread = threading.Thread(target=self._loop, name="job-reconciler", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("[reconciler] pass failed; will retry")
            self._stop.wait(self.interval_secs)

    # ------------------------------------------------------------------
    # one pass
    # ------------------------------------------------------------------
    def run_once(self):
        now = self._clock()
        grace = timedelta(seconds=self.grace_secs)

        present = set()
        for doc in self.db.list_jobs_for_reconciliation():
            snap = snapshot_from_job(doc)
            jobid = snap["jobid"]
            state = self.db.get_reconcile_state(jobid)
            if state is None:
                created = je.parse_utc(snap["created_at"]) if snap["created_at"] else None
                # Only judge occurrences after the node has had time to sync the job.
                # Without a creation time, start from now and never backfill.
                state = {"cursor": (created + grace) if created else now}
            state.update(snap)
            state["last_seen_at"] = now
            self.db.save_reconcile_state(state)
            present.add(jobid)

        for state in self.db.list_reconcile_states():
            self._judge(state, present=state["jobid"] in present, now=now, grace=grace)

    def _horizon(self, state, present, now):
        """Latest time up to which occurrences of this job may be judged."""
        if present:
            return now
        end = je.parse_utc(state["end_date"])
        if now >= end - timedelta(seconds=NATURAL_EXPIRY_WINDOW_SECS):
            return end
        return je.parse_utc(state["last_seen_at"])

    def _occurrences(self, state, lower, upper, zone_name):
        if state["type"] == "cron":
            return je.cron_occurrences(state["cron"], state["start_date"], state["end_date"],
                                       lower, upper, zone_name=zone_name)
        return je.atq_occurrences(state["start_date"], lower, upper)

    def _judge(self, state, present, now, grace):
        jobid = state["jobid"]
        horizon = self._horizon(state, present, now)
        # An occurrence is not due for judgment until its own scheduled duration
        # has elapsed too, not just the grace period after it fired.
        length = timedelta(seconds=state.get("length_secs") or 0)
        upper = min(horizon, now - grace - length)
        lower = max(je.parse_utc(state["cursor"]), now - timedelta(seconds=self.lookback_secs))

        if upper > lower:
            profile = self.db.get_node_profile(state["nodeid"]) or {}
            zone_name = profile.get("timezone")
            if not zone_name:
                # Cron fires in the node's own zone. Judging without it would flag every
                # occurrence in a non-UTC zone. Wait without advancing the cursor, so the
                # recent past is judged correctly once the node reports its zone.
                self._warn_zone_unknown_once(state["nodeid"])
                return
            occurrences = self._occurrences(state, lower, upper, zone_name)
            if occurrences:
                node_last_active = profile.get("last_active")
                starts = self.db.get_run_starts(
                    jobid,
                    occurrences[0] - timedelta(seconds=MATCH_SKEW_SECS),
                    occurrences[-1] + grace)
                reported = self._reported_occurrences(occurrences, starts, grace)
                for occurrence in occurrences:
                    if occurrence in reported:
                        continue
                    self._record_missed(state, occurrence, now, node_last_active)
            state["cursor"] = upper
            self.db.save_reconcile_state(state)

        # A job that is gone and has been judged up to its horizon has nothing left to do.
        if not present and upper >= horizon:
            self.db.delete_reconcile_state(jobid)

    def _warn_zone_unknown_once(self, nodeid):
        if nodeid not in self._zone_warned:
            self._zone_warned.add(nodeid)
            log.warning("[reconciler] node %s has not reported a timezone; "
                        "missed-run tracking for its jobs is paused until it does", nodeid)

    @staticmethod
    def _reported_occurrences(occurrences, starts, grace):
        """Occurrences that have an executor run, each run counted for one occurrence only."""
        skew = timedelta(seconds=MATCH_SKEW_SECS)
        reported = set()
        for start in starts:
            owners = [occurrence for occurrence in occurrences if occurrence <= start + skew]
            if not owners:
                continue
            owner = max(owners)
            if start <= owner + grace:
                reported.add(owner)
        return reported

    def _record_missed(self, state, occurrence, now, node_last_active):
        jobid = state["jobid"]
        reason, detail = je.missed_reason(node_last_active, now, self.stale_secs)
        runid = je.occurrence_runid(jobid, occurrence)
        length_secs = int(state.get("length_secs") or 0)
        message = je.truncate(
            "No executor report within %ds of the experiment's scheduled end "
            "(fire time %s, length %ds). %s."
            % (self.grace_secs, occurrence.isoformat(), length_secs, detail))

        self.db.insert_run_if_absent({
            "runid": runid,
            "jobid": jobid,
            "nodeid": state["nodeid"],
            "userid": state["userid"],
            "start_time": occurrence,
            "end_time": occurrence,
            "last_updated": now,
            "blob_url": "",
            "status": je.STATUS_MISSED,
            "status_message": message,
            "stage": je.STAGE_RECONCILE,
            "reason_code": reason,
        })
        self.db.add_job_event({
            "event_id": je.event_id(jobid, je.STAGE_RECONCILE, occurrence.isoformat()),
            "jobid": jobid,
            "runid": runid,
            "nodeid": state["nodeid"],
            "userid": state["userid"],
            "occurrence": occurrence,
            "stage": je.STAGE_RECONCILE,
            "outcome": je.OUTCOME_MISSED,
            "reason_code": reason,
            "message": message,
            "source": "reconciler",
            "timestamp": now,
        })
        log.warning("[reconciler] MISSED jobid=%s nodeid=%s occurrence=%s reason=%s",
                    jobid, state["nodeid"], occurrence.isoformat(), reason)
