"""Shared vocabulary and pure rules for job and run outcome reporting.

Used by the node executor (stage failures, trigger verdict), the orchestrator
(schedule-time warnings, reconciler) and the tests. Nothing in this module
touches the network or the database, so every rule can be tested directly.
"""

import hashlib
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from croniter import croniter

# ---------------------------------------------------------------------------
# Stages: the step of the lifecycle a record is about
# ---------------------------------------------------------------------------
STAGE_SCHEDULE = "SCHEDULE"
STAGE_CONFIG = "CONFIG"
STAGE_TRIGGER = "TRIGGER"
STAGE_DEPLOY = "DEPLOY"
STAGE_IMAGE_PULL = "IMAGE_PULL"
STAGE_CONTAINER = "CONTAINER"
STAGE_EXECUTE = "EXECUTE"
STAGE_FINISH = "FINISH"
STAGE_RECONCILE = "RECONCILE"

# ---------------------------------------------------------------------------
# Outcomes: what happened at that stage
# ---------------------------------------------------------------------------
OUTCOME_OK = "OK"
OUTCOME_WARN = "WARN"
OUTCOME_FAILED = "FAILED"
OUTCOME_SKIPPED = "SKIPPED"
OUTCOME_MISSED = "MISSED"

# ---------------------------------------------------------------------------
# Reason codes: why an outcome is not OK
# ---------------------------------------------------------------------------
REASON_CONFIG_INVALID = "CONFIG_INVALID"
REASON_CONFIG_FETCH_FAILED = "CONFIG_FETCH_FAILED"
REASON_IMAGE_NOT_FOUND = "IMAGE_NOT_FOUND"
REASON_IMAGE_PULL_DENIED = "IMAGE_PULL_DENIED"
REASON_IMAGE_PULL_FAILED = "IMAGE_PULL_FAILED"
REASON_IMAGE_REFERENCE_INVALID = "IMAGE_REFERENCE_INVALID"
REASON_IMAGE_MISSING_FROM_CONFIG = "IMAGE_MISSING_FROM_CONFIG"
REASON_DEPLOY_FAILED = "DEPLOY_FAILED"
REASON_CONTAINER_START_FAILED = "CONTAINER_START_FAILED"
REASON_CONTAINER_EXIT_NONZERO = "CONTAINER_EXIT_NONZERO"
REASON_CONTAINER_STATE_UNKNOWN = "CONTAINER_STATE_UNKNOWN"
REASON_CONTAINER_STOP_FAILED = "CONTAINER_STOP_FAILED"
REASON_EXECUTE_EXCEPTION = "EXECUTE_EXCEPTION"
REASON_FINISH_FAILED = "FINISH_FAILED"
REASON_ARTIFACT_UPLOAD_FAILED = "ARTIFACT_UPLOAD_FAILED"
REASON_TRIGGER_NOT_MET = "TRIGGER_NOT_MET"
REASON_NODE_OFFLINE = "NODE_OFFLINE"
REASON_NODE_OFFLINE_AT_SCHEDULE = "NODE_OFFLINE_AT_SCHEDULE"
REASON_NODE_TIMEZONE_UNKNOWN = "NODE_TIMEZONE_UNKNOWN"
REASON_NO_EXECUTOR_REPORT = "NO_EXECUTOR_REPORT"

# ---------------------------------------------------------------------------
# Trigger verdicts recorded by the node and reported in the run
# ---------------------------------------------------------------------------
TRIGGER_MET = "MET"
TRIGGER_NOT_MET = "NOT_MET"
TRIGGER_UNKNOWN = "UNKNOWN"
TRIGGER_NOT_CONFIGURED = "NOT_CONFIGURED"

# Run statuses written by the reconciler and the executor's terminal states.
STATUS_FAILED = "FAILED"
STATUS_SKIPPED = "SKIPPED"
STATUS_MISSED = "MISSED"
STATUS_COMPLETE = "COMPLETE"

# A reference in the Docker grammar: lowercase path components, optional tag
# and optional digest. Deliberately conservative; a rejected reference is only
# a warning at schedule time, never a rejection.
_IMAGE_REFERENCE = re.compile(
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?"
    r"(?:@sha256:[a-f0-9]{64})?$")


def truncate(text, limit=500):
    """Shorten free-text messages so one event never bloats a document."""
    text = "" if text is None else str(text).strip()
    if len(text) <= limit:
        return text
    return text[:limit - 3] + "..."


def parse_utc(value):
    """Return a naive UTC datetime for a datetime or ISO-like string."""
    if isinstance(value, datetime):
        parsed = value
    else:
        from dateutil.parser import parse as datetime_parse
        parsed = datetime_parse(str(value))
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def classify_pull_error(message):
    """Map a docker pull error message onto a reason code."""
    text = (message or "").lower()
    if "manifest unknown" in text or "not found" in text or "no such image" in text:
        return REASON_IMAGE_NOT_FOUND
    if "unauthorized" in text or "denied" in text or "authentication required" in text:
        return REASON_IMAGE_PULL_DENIED
    return REASON_IMAGE_PULL_FAILED


def default_reason(stage, message=""):
    """Reason code for a failure at ``stage`` when no finer cause is known."""
    if stage == STAGE_IMAGE_PULL:
        return classify_pull_error(message)
    if stage == STAGE_CONFIG:
        return REASON_CONFIG_INVALID
    if stage == STAGE_DEPLOY:
        return REASON_DEPLOY_FAILED
    if stage == STAGE_CONTAINER:
        return REASON_CONTAINER_START_FAILED
    if stage == STAGE_FINISH:
        return REASON_FINISH_FAILED
    return REASON_EXECUTE_EXCEPTION


def classify_container_exit(exit_code, ttl_stopped, running=False):
    """Decide whether a finished experiment container is a failure.

    Returns ``None`` when the container finished acceptably, otherwise the
    reason code. A container that exits non-zero because the node stopped it
    at its TTL is an expected end of the run, not a failure.
    """
    if running:
        return REASON_CONTAINER_STOP_FAILED
    if exit_code is None:
        return REASON_CONTAINER_STATE_UNKNOWN
    if exit_code == 0:
        return None
    if ttl_stopped:
        return None
    return REASON_CONTAINER_EXIT_NONZERO


def trigger_verdict(record, now, max_age_secs, configured=None):
    """Turn the node's stored trigger evaluation into a verdict for this run.

    :param record: dict ``{"verdict": bool, "evaluated_at": str}`` written by
        the node trigger module, or ``None`` when nothing was recorded.
    :param now: current naive UTC datetime.
    :param max_age_secs: evaluations older than this are treated as unknown.
    :param configured: ``False`` when the job has no trigger, ``True`` when it
        has one, ``None`` when that could not be determined.
    :return: ``(verdict, detail)``
    """
    if configured is False:
        return TRIGGER_NOT_CONFIGURED, ""
    if record is None:
        return TRIGGER_UNKNOWN, "no trigger evaluation was recorded on this node"
    evaluated_at = parse_utc(record.get("evaluated_at"))
    age = (now - evaluated_at).total_seconds()
    if age > max_age_secs:
        return TRIGGER_UNKNOWN, "last trigger evaluation at %s is %ds old (limit %ds)" % (
            evaluated_at.isoformat(), int(age), max_age_secs)
    detail = "evaluated at %s" % evaluated_at.isoformat()
    if record.get("verdict"):
        return TRIGGER_MET, detail
    return TRIGGER_NOT_MET, detail


def trigger_verdict_key(jobid):
    """Redis key holding the node's latest trigger verdict for a job."""
    return "trigger_verdict:%s" % jobid


def should_skip_for_trigger(verdict, gate_enabled):
    """Only skip when the operator enabled gating and the trigger is not met."""
    return bool(gate_enabled) and verdict == TRIGGER_NOT_MET


def is_valid_timezone(name):
    """True if ``name`` is an IANA zone the tzdata on this machine knows."""
    if not name or not isinstance(name, str):
        return False
    try:
        ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        return False
    return True


def local_timezone_name():
    """IANA name of the zone cron runs in on this machine, or ``UTC`` if unknown.

    Cron entries fire in this zone, so the orchestrator needs it to judge fire times.
    """
    env_zone = (os.environ.get("TZ") or "").strip().lstrip(":")
    if is_valid_timezone(env_zone):
        return env_zone
    try:
        target = os.path.realpath("/etc/localtime")
    except OSError:
        return "UTC"
    if "zoneinfo/" in target:
        name = target.split("zoneinfo/", 1)[1]
        if is_valid_timezone(name):
            return name
    return "UTC"


def cron_occurrences(cron_string, start, end, lower_excl, upper_incl, zone_name="UTC", limit=5000):
    """Fire times of a 5-field cron string in ``(lower_excl, upper_incl]``.

    The cron string is read in ``zone_name``, the zone the node's cron runs in,
    because that is where it fires. Returned times are naive UTC, matching the
    run records. Only times within ``[start, end]`` count.
    """
    zone = ZoneInfo(zone_name if is_valid_timezone(zone_name) else "UTC")
    lower = parse_utc(lower_excl)
    upper = parse_utc(upper_incl)
    lo = max(lower, parse_utc(start))
    hi = min(upper, parse_utc(end))
    if lo > hi:
        return []
    base = (lo - timedelta(seconds=1)).replace(tzinfo=timezone.utc).astimezone(zone)
    iterator = croniter(cron_string, base)
    occurrences = []
    while len(occurrences) < limit:
        fire = iterator.get_next(datetime)
        occurrence = fire.astimezone(timezone.utc).replace(tzinfo=None)
        if occurrence > hi:
            break
        if occurrence > lower and occurrence >= parse_utc(start):
            occurrences.append(occurrence)
    return occurrences


def atq_occurrences(start, lower_excl, upper_incl):
    """A one-shot job has a single occurrence at its start time."""
    start = parse_utc(start)
    if parse_utc(lower_excl) < start <= parse_utc(upper_incl):
        return [start]
    return []


def occurrence_runid(jobid, occurrence):
    """Deterministic run id for a reconciler-written row, so reruns upsert."""
    digest = hashlib.sha256(("%s|%s" % (jobid, parse_utc(occurrence).isoformat())).encode()).hexdigest()
    return "missed-%s" % digest[:24]


def event_id(jobid, stage, occurrence="", discriminator=""):
    """Deterministic id for a job event, so repeated writes are idempotent."""
    raw = "|".join([jobid, stage, str(occurrence), str(discriminator)])
    return hashlib.sha256(raw.encode()).hexdigest()


def missed_reason(node_last_active, now, stale_secs):
    """Reason and detail for an occurrence that no executor reported on."""
    if node_last_active is None:
        return REASON_NODE_OFFLINE, "the node has no heartbeat on record"
    last = parse_utc(node_last_active)
    age = (now - last).total_seconds()
    if age > stale_secs:
        return REASON_NODE_OFFLINE, "last heartbeat %s (%ds ago)" % (last.isoformat(), int(age))
    return REASON_NO_EXECUTOR_REPORT, "node is reporting (last heartbeat %s) but the executor never reported" % (
        last.isoformat())


def schedule_warnings(config_yaml, node_last_active, now, stale_secs, node_timezone):
    """Problems visible at schedule time that would make the run fail later.

    Returns a list of ``(reason_code, message)``. These never block scheduling.
    """
    warnings = []

    if node_last_active is None or (now - parse_utc(node_last_active)).total_seconds() > stale_secs:
        warnings.append((
            REASON_NODE_OFFLINE_AT_SCHEDULE,
            "The node has not reported recently. Each occurrence that the node does not "
            "report within the grace period will be recorded as MISSED in the runs tab."))

    if not node_timezone:
        warnings.append((
            REASON_NODE_TIMEZONE_UNKNOWN,
            "The node has not reported its timezone yet, so missed-run tracking waits for it. "
            "Results for this job are recorded once the node reports in its next heartbeat."))

    if not config_yaml or not str(config_yaml).strip():
        warnings.append((REASON_CONFIG_INVALID,
                         "The experiment config is empty; the node cannot start this job."))
        return warnings

    try:
        parsed = yaml.safe_load(config_yaml)
    except yaml.YAMLError as exc:
        warnings.append((REASON_CONFIG_INVALID,
                         "The experiment config is not valid YAML: %s" % truncate(exc, 200)))
        return warnings

    docker_block = parsed.get("docker") if isinstance(parsed, dict) else None
    image = docker_block.get("image") if isinstance(docker_block, dict) else None
    if not image:
        warnings.append((REASON_IMAGE_MISSING_FROM_CONFIG,
                         "The experiment config has no docker.image; the node cannot pull an image."))
    elif not _IMAGE_REFERENCE.match(str(image)):
        warnings.append((REASON_IMAGE_REFERENCE_INVALID,
                         "The docker image reference '%s' does not look valid; "
                         "the pull on the node is likely to fail." % truncate(image, 200)))
    return warnings
