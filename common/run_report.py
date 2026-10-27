"""The plain-text diagnostics report that travels with every run.

A run's artifacts are uploaded as a zip, so when a run fails the person who
scheduled it has nothing to read until they download and unpack it -- and if the
failure happened before the archive was built, there is nothing to download at
all. Both the executor and the node's dispatcher therefore build a single text
file here and upload it next to the artifacts, under the same container and
path. It is the one thing that is always there to explain a run.

Formatting is pure. ``upload_text`` is the only function that touches the
network, and it never raises: a report that cannot be uploaded must not turn a
run that merely failed into a run that reported nothing.
"""

import logging
import os

log = logging.getLogger(__name__)

# Each captured log is tailed, not inlined whole: a chatty experiment can write
# hundreds of megabytes, and the report has to stay readable and cheap to fetch.
DEFAULT_TAIL_BYTES = 256 * 1024

REPORT_SUFFIX = "-report.txt"


def report_blob_path(remote_path, runid):
    """Blob path of a run's report, next to that run's artifact archive."""
    return "%s/%s%s" % (remote_path.rstrip("/"), runid, REPORT_SUFFIX)


def tail(path, max_bytes=DEFAULT_TAIL_BYTES):
    """Last ``max_bytes`` of a text file, or a note saying why it is not there.

    Never raises: this runs while a failure is already being reported.
    """
    try:
        size = os.path.getsize(path)
    except OSError as exc:
        return "(not available: %s)" % exc
    if size == 0:
        return "(empty)"
    try:
        with open(path, "rb") as handle:
            if size > max_bytes:
                handle.seek(size - max_bytes)
            data = handle.read()
    except OSError as exc:
        return "(could not be read: %s)" % exc
    text = data.decode("utf-8", errors="replace")
    if size > max_bytes:
        text = ("(truncated: showing the last %d of %d bytes)\n" % (max_bytes, size)) + text
    return text


def build_report(title, meta, sections):
    """Render the report.

    :param title: one line naming what this report is about.
    :param meta: ordered ``(label, value)`` pairs. A value of ``None`` is kept
        and shown as ``-``, so a missing field is visibly missing rather than
        silently absent.
    :param sections: ordered ``(heading, body)`` pairs. A section with an empty
        body is dropped.
    """
    rule = "=" * 78
    lines = [rule, title, rule, ""]

    width = max([len(str(label)) for label, _ in meta] or [0])
    for label, value in meta:
        shown = "-" if value is None or value == "" else value
        lines.append("%-*s : %s" % (width, label, shown))

    for heading, body in sections:
        body = "" if body is None else str(body).strip()
        if not body:
            continue
        lines += ["", "-" * 78, heading, "-" * 78, body]

    lines.append("")
    return "\n".join(lines)


def upload_text(connection_string, container, blob_path, text):
    """Upload a report and return a download URL, or ``""`` if that failed.

    Imports the storage client lazily so that a node without the Azure SDK
    installed still reports its failures over gRPC.
    """
    if not connection_string or not container:
        log.warning("[report] no Azure credentials for this run; report not uploaded "
                    "(blob=%s)", blob_path)
        return ""
    try:
        from common.utils import StorageDirectoryClient

        client = StorageDirectoryClient(connection_string, container)
        client.client.upload_blob(name=blob_path, data=text.encode("utf-8"), overwrite=True)
        log.info("[report] uploaded report container=%s blob=%s (%d bytes)",
                 container, blob_path, len(text))
    except Exception:
        log.exception("[report] FAILED to upload report container=%s blob=%s",
                      container, blob_path)
        return ""
    try:
        return client.get_sas_url(blob_path)
    except Exception:
        log.exception("[report] uploaded report but could not sign a URL for blob=%s", blob_path)
        return ""
