"""Node bandwidth and data-volume policy: the shared document and the pure rules.

A node owner decides how much of their link LEOScope may use. That decision is
stored on the node record as ``bandwidth_limits_json`` and is read by three
places that must agree on it:

* the website, which lets the owner edit it and shows users which nodes are
  limited before they schedule anything;
* the orchestrator, which stores it;
* the node's executor, which samples the experiment container once a second and
  stops the measurement when the policy is breached.

Nothing here touches the network, Docker or the database, so every rule below
can be tested directly -- the same arrangement as :mod:`common.job_events`.

The document
------------

Version 2 is an object::

    {
      "version": 2,
      "enforcement": {"enabled": true, "sample_interval_secs": 1,
                      "breach_grace_secs": 5},
      "windows": [{"id": "w1", "days": ["Mon"], "start_time": "09:00",
                   "end_time": "17:00", "download_mbps": 50.0,
                   "upload_mbps": 10.0}],
      "default_limits": {"download_mbps": null, "upload_mbps": null},
      "run_caps": {"max_download_mb": null, "max_upload_mb": null,
                   "max_total_mb": 500.0, "max_artifact_mb": 200.0}
    }

Version 1 -- what nodes configured before this module hold -- is a bare list of
windows. It is read as a v2 document with enforcement *off*: those windows were
recorded when nothing acted on them, and switching a node from advisory to
enforcing without its owner asking would start stopping runs on their behalf.
``ENFORCEMENT_DEFAULT`` only applies to documents that say nothing at all.
"""

import json
import os
import re
from datetime import datetime, timedelta

DAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

VERSION = 2

# A rate cap is read in megabits per second, the unit owners think in and the
# unit the UI shows. Internally everything is bits per second.
BITS_PER_MBIT = 1000000
BYTES_PER_MB = 1000000

# Defaults for a document that does not set them.
DEFAULT_SAMPLE_INTERVAL_SECS = 1
DEFAULT_BREACH_GRACE_SECS = 5
MAX_SAMPLE_INTERVAL_SECS = 60
MAX_BREACH_GRACE_SECS = 300

#: Whether a policy that does not say otherwise is enforced. A v1 list always
#: reads as advisory regardless of this, see the module docstring.
ENFORCEMENT_DEFAULT = (os.environ.get(
    "LEOSCOPE_BANDWIDTH_ENFORCEMENT_DEFAULT", "true").strip().lower()
    in ("1", "true", "yes", "on"))

RATE_FIELDS = ("download_mbps", "upload_mbps")
CAP_FIELDS = ("max_download_mb", "max_upload_mb", "max_total_mb", "max_artifact_mb")

_TIME = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")

# Reason codes for a stopped measurement. They are re-exported by
# common.job_events so that the executor has one import for reason codes.
REASON_BANDWIDTH_RATE_EXCEEDED = "BANDWIDTH_RATE_EXCEEDED"
REASON_BANDWIDTH_VOLUME_EXCEEDED = "BANDWIDTH_VOLUME_EXCEEDED"
REASON_ARTIFACT_SIZE_EXCEEDED = "ARTIFACT_SIZE_EXCEEDED"


class PolicyError(ValueError):
    """A policy document the owner cannot have meant. Carries one message per problem."""

    def __init__(self, errors):
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


# ---------------------------------------------------------------------------
# reading and writing the document
# ---------------------------------------------------------------------------
def _optional_number(value, minimum=0.0):
    """A positive number, or ``None`` for "no limit". Blank and 0 both mean none.

    The editor sends "" for a field the owner cleared, and a stored 0 from an
    older document would otherwise read as "zero megabits allowed" and stop
    every run on the node.
    """
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise PolicyError(["%r is not a number" % (value,)])
    if number != number or number in (float("inf"), float("-inf")):
        raise PolicyError(["%r is not a finite number" % (value,)])
    if number <= minimum:
        return None
    return number


def _bounded_int(value, default, low, high):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(low, min(high, number))


def normalise_time(value, fallback):
    text = str(value or "").strip()
    if _TIME.match(text):
        hours, minutes = text.split(":")
        return "%02d:%02d" % (int(hours), int(minutes))
    return fallback


def _minutes(hhmm):
    hours, minutes = hhmm.split(":")
    return int(hours) * 60 + int(minutes)


def normalise_days(value):
    """The subset of :data:`DAYS` in ``value``, in week order, without repeats."""
    if isinstance(value, str):
        raw = [part.strip() for part in value.split(",")]
    elif isinstance(value, (list, tuple, set)):
        raw = list(value)
    else:
        return []
    wanted = {str(item).strip().title()[:3] for item in raw if str(item).strip()}
    return [day for day in DAYS if day in wanted]


def normalise_window(raw, index=0):
    raw = raw if isinstance(raw, dict) else {}
    window = {
        "id": str(raw.get("id") or "w%d" % (index + 1)),
        "days": normalise_days(raw.get("days")) or list(DAYS),
        "start_time": normalise_time(raw.get("start_time"), "00:00"),
        "end_time": normalise_time(raw.get("end_time"), "23:59"),
    }
    for field in RATE_FIELDS:
        window[field] = _optional_number(raw.get(field))
    return window


def normalise(raw):
    """Read any stored or submitted form of the document into the v2 shape.

    Accepts ``None``, a JSON string, a v1 list of windows, or a v2 object, and
    never raises on a stored value: a document that cannot be parsed reads as
    an empty, unenforced policy, which leaves the node unrestricted rather than
    stopping its runs over a corrupt field. Submitted documents are checked by
    :func:`validate` before they are stored.
    """
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except ValueError:
            raw = []
    if raw is None:
        raw = []

    legacy_list = isinstance(raw, list)
    body = {"windows": raw} if legacy_list else (raw if isinstance(raw, dict) else {})

    # A str is iterable, so a document whose "windows" is a stray string would
    # otherwise normalise into one window per character.
    windows_raw = body.get("windows")
    windows_raw = windows_raw if isinstance(windows_raw, list) else []

    windows = []
    for index, item in enumerate(windows_raw):
        try:
            windows.append(normalise_window(item, index))
        except PolicyError:
            continue

    enforcement_raw = body.get("enforcement")
    enforcement_raw = enforcement_raw if isinstance(enforcement_raw, dict) else {}
    if "enabled" in enforcement_raw:
        enabled = bool(enforcement_raw["enabled"])
    elif legacy_list:
        # v1 windows were advisory. See the module docstring.
        enabled = False
    else:
        enabled = ENFORCEMENT_DEFAULT

    defaults_raw = body.get("default_limits")
    defaults_raw = defaults_raw if isinstance(defaults_raw, dict) else {}
    caps_raw = body.get("run_caps")
    caps_raw = caps_raw if isinstance(caps_raw, dict) else {}

    def safe(mapping, field):
        try:
            return _optional_number(mapping.get(field))
        except PolicyError:
            return None

    return {
        "version": VERSION,
        "enforcement": {
            "enabled": enabled,
            "sample_interval_secs": _bounded_int(
                enforcement_raw.get("sample_interval_secs"),
                DEFAULT_SAMPLE_INTERVAL_SECS, 1, MAX_SAMPLE_INTERVAL_SECS),
            "breach_grace_secs": _bounded_int(
                enforcement_raw.get("breach_grace_secs"),
                DEFAULT_BREACH_GRACE_SECS, 0, MAX_BREACH_GRACE_SECS),
        },
        "windows": windows,
        "default_limits": {field: safe(defaults_raw, field) for field in RATE_FIELDS},
        "run_caps": {field: safe(caps_raw, field) for field in CAP_FIELDS},
    }


def to_json(policy):
    """Serialise a normalised policy for storage on the node record."""
    return json.dumps(normalise(policy), sort_keys=True)


def validate(raw):
    """Problems with a *submitted* document, as a list of messages for the owner.

    Empty means the document is storable. Unlike :func:`normalise`, this does
    not quietly repair anything: an owner who typed "9am" into a time field is
    told, not silently given midnight.
    """
    errors = []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except ValueError as exc:
            return ["The bandwidth policy is not valid JSON: %s" % exc]
    body = {"windows": raw} if isinstance(raw, list) else raw
    if not isinstance(body, dict):
        return ["The bandwidth policy must be an object or a list of windows."]

    windows = body.get("windows") or []
    if not isinstance(windows, list):
        return ["The bandwidth policy's windows must be a list."]

    seen_ids = set()
    for index, window in enumerate(windows):
        label = "Window %d" % (index + 1)
        if not isinstance(window, dict):
            errors.append("%s is not a set of fields." % label)
            continue

        window_id = str(window.get("id") or "")
        if window_id and window_id in seen_ids:
            errors.append("%s repeats the id %r." % (label, window_id))
        seen_ids.add(window_id)

        if not normalise_days(window.get("days")):
            errors.append("%s has no days selected; pick at least one day." % label)

        for field, name in (("start_time", "start time"), ("end_time", "end time")):
            if not _TIME.match(str(window.get(field) or "").strip()):
                errors.append("%s has an invalid %s; use HH:MM (24-hour)." % (label, name))

        for field in RATE_FIELDS:
            value = window.get(field)
            if value in (None, ""):
                continue
            try:
                number = float(value)
            except (TypeError, ValueError):
                errors.append("%s has a non-numeric %s." % (label, field))
                continue
            if number < 0:
                errors.append("%s has a negative %s." % (label, field))

        if all(window.get(field) in (None, "") for field in RATE_FIELDS):
            errors.append(
                "%s sets no download or upload limit, so it would have no effect. "
                "Set a limit or remove the window." % label)

    for mapping, fields, what in ((body.get("default_limits") or {}, RATE_FIELDS, "default limit"),
                                  (body.get("run_caps") or {}, CAP_FIELDS, "per-run cap")):
        if not isinstance(mapping, dict):
            errors.append("The %ss must be a set of fields." % what)
            continue
        for field in fields:
            value = mapping.get(field)
            if value in (None, ""):
                continue
            try:
                if float(value) < 0:
                    errors.append("The %s %s cannot be negative." % (what, field))
            except (TypeError, ValueError):
                errors.append("The %s %s is not a number." % (what, field))

    enforcement = body.get("enforcement")
    if enforcement is not None and not isinstance(enforcement, dict):
        errors.append("The enforcement settings must be a set of fields.")
    return errors


# ---------------------------------------------------------------------------
# which limits apply right now
# ---------------------------------------------------------------------------
def window_matches(window, moment):
    """Does ``window`` cover ``moment``? ``moment`` is a local-time datetime.

    A window whose end is at or before its start runs overnight: 22:00-06:00 is
    read as 22:00 to midnight on each selected day, and midnight to 06:00 on
    the following morning. The day list names the day the window *starts* on,
    which is how an owner reading "Fri 22:00-06:00" expects it to behave.
    """
    days = window.get("days") or []
    start = _minutes(window["start_time"])
    end = _minutes(window["end_time"])
    today = DAYS[moment.weekday()]
    now = moment.hour * 60 + moment.minute

    if end > start:
        return today in days and start <= now <= end

    # overnight, or a window that covers the whole day when start == end
    if today in days and now >= start:
        return True
    yesterday = DAYS[(moment.weekday() - 1) % 7]
    return yesterday in days and now <= end


def active_window(policy, moment):
    """The first window covering ``moment``, or ``None``.

    Windows are checked in the order the owner arranged them, so an owner who
    wants a tighter night-time rule inside a broad all-week rule puts it first.
    """
    for window in normalise(policy)["windows"]:
        if window_matches(window, moment):
            return window
    return None


def effective_limits(policy, moment):
    """What the node may use at ``moment``.

    Byte caps are per run and do not vary by window; rate caps come from the
    active window, falling back to ``default_limits``.
    """
    policy = normalise(policy)
    window = active_window(policy, moment)
    source = window or policy["default_limits"]
    caps = policy["run_caps"]

    def megabytes(field):
        value = caps.get(field)
        return int(value * BYTES_PER_MB) if value else None

    def bps(field):
        value = source.get(field)
        return int(value * BITS_PER_MBIT) if value else None

    return {
        "enforced": bool(policy["enforcement"]["enabled"]),
        "sample_interval_secs": policy["enforcement"]["sample_interval_secs"],
        "breach_grace_secs": policy["enforcement"]["breach_grace_secs"],
        "window_id": window["id"] if window else None,
        "window_label": describe_window(window) if window else "",
        "download_bps": bps("download_mbps"),
        "upload_bps": bps("upload_mbps"),
        "max_download_bytes": megabytes("max_download_mb"),
        "max_upload_bytes": megabytes("max_upload_mb"),
        "max_total_bytes": megabytes("max_total_mb"),
        "max_artifact_bytes": megabytes("max_artifact_mb"),
    }


def has_any_limit(policy):
    """True when the policy restricts anything at all, enforced or not."""
    policy = normalise(policy)
    if any(policy["run_caps"].get(field) for field in CAP_FIELDS):
        return True
    if any(policy["default_limits"].get(field) for field in RATE_FIELDS):
        return True
    return any(window.get(field) for window in policy["windows"] for field in RATE_FIELDS)


def is_enforced(policy):
    return bool(normalise(policy)["enforcement"]["enabled"]) and has_any_limit(policy)


# ---------------------------------------------------------------------------
# human-readable descriptions, shared by the website and the run report
# ---------------------------------------------------------------------------
def format_bytes(value):
    """Bytes in the largest unit that keeps the number readable.

    The weekly figures a node's policy adds up to reach terabytes, so the scale
    runs that far: "%.3g" of 4.5e12 bytes in GB would print as "4.54e+03 GB".
    """
    if value is None:
        return "unlimited"
    value = float(value)
    for unit, scale in (("PB", 1e15), ("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("kB", 1e3)):
        if value >= scale:
            return "%.3g %s" % (value / scale, unit)
    return "%d B" % int(value)


def format_rate(bps):
    if bps is None:
        return "unlimited"
    bps = float(bps)
    for unit, scale in (("Gbps", 1e9), ("Mbps", 1e6), ("kbps", 1e3)):
        if bps >= scale:
            return "%.3g %s" % (bps / scale, unit)
    return "%d bps" % int(bps)


def _day_phrase(days):
    days = list(days or [])
    if len(days) == 7:
        return "Every day"
    if days == ["Mon", "Tue", "Wed", "Thu", "Fri"]:
        return "Weekdays"
    if days == ["Sat", "Sun"]:
        return "Weekends"
    return ", ".join(days) if days else "No days"


def describe_window(window):
    """One line an owner can check at a glance, e.g. ``Weekdays 09:00-17:00: down 50 Mbps``."""
    if not window:
        return ""
    parts = []
    for field, label in (("download_mbps", "down"), ("upload_mbps", "up")):
        value = window.get(field)
        if value:
            parts.append("%s %s" % (label, format_rate(value * BITS_PER_MBIT)))
    limits = ", ".join(parts) if parts else "no rate limit"
    overnight = _minutes(window["end_time"]) <= _minutes(window["start_time"])
    return "%s %s-%s%s: %s" % (_day_phrase(window.get("days")), window["start_time"],
                               window["end_time"], " (overnight)" if overnight else "",
                               limits)


def describe(policy):
    """Every line of the policy, for a tooltip or the run report. May be empty."""
    policy = normalise(policy)
    lines = [describe_window(window) for window in policy["windows"]]

    defaults = policy["default_limits"]
    if any(defaults.get(field) for field in RATE_FIELDS):
        lines.append("Outside those windows: %s" % ", ".join(
            "%s %s" % (label, format_rate(defaults[field] * BITS_PER_MBIT))
            for field, label in (("download_mbps", "down"), ("upload_mbps", "up"))
            if defaults.get(field)))

    caps = policy["run_caps"]
    cap_labels = (("max_download_mb", "downloaded"), ("max_upload_mb", "uploaded"),
                  ("max_total_mb", "transferred in total"), ("max_artifact_mb", "of collected data"))
    for field, label in cap_labels:
        if caps.get(field):
            lines.append("Each run stops after %s %s"
                         % (format_bytes(caps[field] * BYTES_PER_MB), label))

    if lines and not policy["enforcement"]["enabled"]:
        lines.append("Enforcement is off: these limits are advisory and no run is stopped.")
    return lines


def summarise(policy):
    """A short phrase for a node list badge."""
    policy = normalise(policy)
    if not has_any_limit(policy):
        return "No bandwidth limits"
    if not policy["enforcement"]["enabled"]:
        return "Limits configured (advisory)"
    counts = []
    rate_windows = [window for window in policy["windows"]
                    if any(window.get(field) for field in RATE_FIELDS)]
    if rate_windows:
        counts.append("%d rate window%s" % (len(rate_windows), "" if len(rate_windows) == 1 else "s"))
    caps = [field for field in CAP_FIELDS if policy["run_caps"].get(field)]
    if caps:
        counts.append("%d per-run cap%s" % (len(caps), "" if len(caps) == 1 else "s"))
    return "Enforced: " + ", ".join(counts)


# ---------------------------------------------------------------------------
# what a node has dedicated to LEOScope
# ---------------------------------------------------------------------------
# A Monday at midnight. Used only to walk one whole week of slots, so the
# weekday arithmetic in window_matches lines up with the day names.
_WEEK_START = datetime(2024, 1, 1)
_SLOT_MINUTES = 15
_SLOTS_PER_WEEK = 7 * 24 * 60 // _SLOT_MINUTES


def contribution(policy):
    """How much of this node's connection its owner has dedicated to LEOScope.

    This is the *offer*, not the usage: the ceiling the policy allows. It is
    what a node owner wants to see when they ask what they are contributing,
    and what a user comparing nodes wants to see before scheduling.

    The week is walked in 15-minute slots, each resolved through the same
    :func:`active_window` the node uses, so overlapping windows, overnight
    windows and the fallback rates are all counted exactly as they will be
    applied rather than re-derived by a second set of rules.

    ``weekly_bytes`` is ``None`` when any part of the week has no rate cap at
    all: the honest answer there is "unmetered", not a number invented by
    treating an uncapped hour as zero.

    :return: a mapping with ``peak_download_mbps``, ``peak_upload_mbps``,
        ``limited_hours_per_week``, ``unlimited_hours_per_week``,
        ``weekly_bytes``, ``metered``, ``per_run_total_bytes`` and a
        ready-to-show ``headline``.
    """
    policy = normalise(policy)
    slot_seconds = _SLOT_MINUTES * 60

    peak_download = peak_upload = 0.0
    unlimited_slots = 0
    limited_slots = 0
    weekly_bits = 0.0
    metered = True

    for index in range(_SLOTS_PER_WEEK):
        moment = _WEEK_START + timedelta(minutes=index * _SLOT_MINUTES)
        window = active_window(policy, moment)
        source = window or policy["default_limits"]
        download, upload = source.get("download_mbps"), source.get("upload_mbps")

        if download is None and upload is None:
            unlimited_slots += 1
            metered = False
            continue

        limited_slots += 1
        # One direction capped and the other not still leaves the week
        # unmetered overall, but the capped direction is counted: an owner who
        # caps only their uplink has dedicated a real, known amount of it.
        if download is None or upload is None:
            metered = False
        peak_download = max(peak_download, download or 0.0)
        peak_upload = max(peak_upload, upload or 0.0)
        weekly_bits += ((download or 0.0) + (upload or 0.0)) * BITS_PER_MBIT * slot_seconds

    hours = lambda slots: round(slots * _SLOT_MINUTES / 60.0, 2)
    caps = policy["run_caps"]
    per_run_total = caps.get("max_total_mb")

    result = {
        "peak_download_mbps": peak_download or None,
        "peak_upload_mbps": peak_upload or None,
        "limited_hours_per_week": hours(limited_slots),
        "unlimited_hours_per_week": hours(unlimited_slots),
        "weekly_bytes": int(weekly_bits / 8) if metered else None,
        "metered": metered,
        "per_run_total_bytes": int(per_run_total * BYTES_PER_MB) if per_run_total else None,
        "per_run_artifact_bytes": (int(caps["max_artifact_mb"] * BYTES_PER_MB)
                                   if caps.get("max_artifact_mb") else None),
    }
    result["headline"] = _contribution_headline(result)
    return result


def _contribution_headline(figures):
    """One line for a card: the rate offered and the volume it adds up to."""
    rates = []
    if figures["peak_download_mbps"]:
        rates.append("%s down" % format_rate(figures["peak_download_mbps"] * BITS_PER_MBIT))
    if figures["peak_upload_mbps"]:
        rates.append("%s up" % format_rate(figures["peak_upload_mbps"] * BITS_PER_MBIT))

    if not rates:
        return "Unmetered: no rate limit set"

    rate_text = "Up to " + " / ".join(rates)
    if figures["weekly_bytes"] is not None:
        return "%s, about %s a week" % (rate_text, format_bytes(figures["weekly_bytes"]))
    if figures["limited_hours_per_week"]:
        return "%s for %g h a week, unmetered the rest of the time" % (
            rate_text, figures["limited_hours_per_week"])
    return rate_text


def aggregate_contribution(policies):
    """Add up what a set of nodes has dedicated, for a fleet-level summary.

    Nodes with no rate cap cannot be added to a weekly total, so they are
    counted separately and reported as unmetered rather than quietly left out
    of a number that would then read as the whole fleet's contribution.
    """
    totals = {
        "nodes": 0,
        "metered_nodes": 0,
        "unmetered_nodes": 0,
        "peak_download_mbps": 0.0,
        "peak_upload_mbps": 0.0,
        "weekly_bytes": 0,
        "limited_hours_per_week": 0.0,
    }

    for policy in policies:
        figures = contribution(policy)
        totals["nodes"] += 1
        totals["peak_download_mbps"] += figures["peak_download_mbps"] or 0.0
        totals["peak_upload_mbps"] += figures["peak_upload_mbps"] or 0.0
        totals["limited_hours_per_week"] += figures["limited_hours_per_week"]
        if figures["weekly_bytes"] is None:
            totals["unmetered_nodes"] += 1
        else:
            totals["metered_nodes"] += 1
            totals["weekly_bytes"] += figures["weekly_bytes"]

    totals["peak_download_mbps"] = round(totals["peak_download_mbps"], 3) or None
    totals["peak_upload_mbps"] = round(totals["peak_upload_mbps"], 3) or None
    totals["limited_hours_per_week"] = round(totals["limited_hours_per_week"], 2)
    return totals


# ---------------------------------------------------------------------------
# the per-sample decision
# ---------------------------------------------------------------------------
def new_state():
    """Bookkeeping carried between samples by :func:`evaluate`."""
    return {"over_download_secs": 0.0, "over_upload_secs": 0.0}


def evaluate(limits, sample, state):
    """Decide whether this sample ends the measurement.

    :param limits: the mapping from :func:`effective_limits`.
    :param sample: ``{"download_bps", "upload_bps", "download_bytes",
        "upload_bytes", "artifact_bytes", "interval_secs"}``. Any value may be
        ``None`` when it could not be read; a reading that is missing never
        counts as a breach.
    :param state: from :func:`new_state`, mutated in place so that a rate cap
        is only breached once it has been exceeded for ``breach_grace_secs``.
    :return: ``(reason_code, message)``, or ``None`` when the run may continue.

    A volume cap is breached the moment it is passed -- the bytes are already
    spent. A rate cap needs to be sustained: a single second over the line is
    the burst at the start of almost every throughput test, and stopping those
    would make an owner's 50 Mbps ceiling behave like a ban on measuring.
    """
    if not limits.get("enforced"):
        return None

    interval = sample.get("interval_secs") or 0.0

    for key, cap_key, label in (
            ("download_bytes", "max_download_bytes", "downloaded"),
            ("upload_bytes", "max_upload_bytes", "uploaded")):
        used, cap = sample.get(key), limits.get(cap_key)
        if cap and used is not None and used > cap:
            return (REASON_BANDWIDTH_VOLUME_EXCEEDED,
                    "the run %s %s, over this node's per-run limit of %s"
                    % (label, format_bytes(used), format_bytes(cap)))

    total_cap = limits.get("max_total_bytes")
    if total_cap:
        down, up = sample.get("download_bytes"), sample.get("upload_bytes")
        if down is not None or up is not None:
            total = (down or 0) + (up or 0)
            if total > total_cap:
                return (REASON_BANDWIDTH_VOLUME_EXCEEDED,
                        "the run transferred %s in total, over this node's per-run limit of %s"
                        % (format_bytes(total), format_bytes(total_cap)))

    artifact_cap = limits.get("max_artifact_bytes")
    artifact = sample.get("artifact_bytes")
    if artifact_cap and artifact is not None and artifact > artifact_cap:
        return (REASON_ARTIFACT_SIZE_EXCEEDED,
                "the data collected by the container reached %s, over this node's "
                "per-run limit of %s" % (format_bytes(artifact), format_bytes(artifact_cap)))

    grace = limits.get("breach_grace_secs", DEFAULT_BREACH_GRACE_SECS)
    for rate_key, cap_key, state_key, label in (
            ("download_bps", "download_bps", "over_download_secs", "download"),
            ("upload_bps", "upload_bps", "over_upload_secs", "upload")):
        cap = limits.get(cap_key)
        rate = sample.get(rate_key)
        if not cap or rate is None:
            state[state_key] = 0.0
            continue
        if rate > cap:
            state[state_key] += interval
            if state[state_key] > grace:
                return (REASON_BANDWIDTH_RATE_EXCEEDED,
                        "%s stayed above this node's limit of %s for %gs (reached %s)"
                        % (label, format_rate(cap), state[state_key], format_rate(rate)))
        else:
            state[state_key] = 0.0

    return None
