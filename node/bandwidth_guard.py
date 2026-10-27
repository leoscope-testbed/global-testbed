#!/usr/bin/python3
"""Per-second bandwidth and data-volume enforcement for a running experiment.

The node owner's policy lives on the node record (see
:mod:`common.bandwidth_policy`). This module is what makes it real: while an
experiment container runs, a guard thread samples Docker's own counters for
that container once a second, works out the download and upload rates and the
bytes spent so far, measures how much data the container has written into the
run's artifact directory, and stops the container the moment the policy says
the measurement has to end.

Two things are kept apart on purpose:

* :mod:`common.bandwidth_policy` holds every rule and no I/O, so the decisions
  are tested directly;
* this module holds the I/O -- Docker, the filesystem, the clock, the thread --
  and makes no decisions of its own.

Every sample is also appended to ``bandwidth.csv`` in the run's workdir, so the
per-second trace is uploaded with the run's artifacts and both the owner and
whoever scheduled the experiment can see exactly what the link was asked to do.

What can and cannot be measured
-------------------------------

Docker reports per-container network counters only for containers on a Docker
network. An experiment container started with ``network_mode: host`` shares the
node's network namespace and Docker reports no counters for it; there is then
no way to tell the experiment's traffic from everything else on the node, so
the rate and transfer caps are reported as unmeasurable and are *not* enforced,
while the artifact-size cap still is. The node's executor config sets a bridge
network (``docker.execute.network``), which is the measurable case; a node that
has been switched to host networking says so in the run report rather than
silently letting an experiment past its owner's ceiling.
"""

import csv
import os
import threading
from datetime import datetime
from time import monotonic

from common import bandwidth_policy as bp

# How often the artifact directory is measured, as a multiple of the sample
# interval. Walking a directory is far more expensive than reading two
# counters, and collected data grows over seconds, not milliseconds.
ARTIFACT_SAMPLE_EVERY = 5

CSV_FIELDS = ("timestamp", "elapsed_secs", "interval_secs",
              "rx_bytes_total", "tx_bytes_total", "download_bps", "upload_bps",
              "artifact_bytes", "window_id", "download_bps_limit", "upload_bps_limit")


def directory_size(path):
    """Bytes held under ``path``, or ``None`` if it cannot be read.

    Symlinks are not followed and files that vanish while walking are skipped:
    the experiment is writing into this tree as it is measured.
    """
    if not path:
        return None
    total = 0
    try:
        for root, _dirs, files in os.walk(path, followlinks=False):
            for name in files:
                try:
                    stat = os.lstat(os.path.join(root, name))
                except OSError:
                    continue
                total += stat.st_size
    except OSError:
        return None
    return total


class DockerStatsReader:
    """Cumulative network counters for one container, read from the Docker API.

    ``read()`` returns ``{"rx_bytes", "tx_bytes"}``, or ``None`` when the
    counters are not available -- the container has gone, or it is on the host
    network namespace and Docker reports no interfaces for it.
    """

    def __init__(self, docker_client, container_name, log):
        self.docker_client = docker_client
        self.container_name = container_name
        self.log = log
        self._one_shot = True
        self._warned = False

    def _stats(self, container):
        # one_shot skips the second read the daemon otherwise does to compute
        # CPU percentages, which is a whole sample interval of delay we would
        # pay once a second for numbers we do not use. Older docker-py and
        # older API versions do not accept it.
        if self._one_shot:
            try:
                return container.stats(stream=False, one_shot=True)
            except TypeError:
                self._one_shot = False
            except Exception as exc:
                if "one-shot" in str(exc).lower() or "one_shot" in str(exc).lower():
                    self._one_shot = False
                else:
                    raise
        return container.stats(stream=False)

    def read(self):
        try:
            container = self.docker_client.containers.get(self.container_name)
            stats = self._stats(container)
        except Exception as exc:
            if not self._warned:
                self._warned = True
                self.log.warning("[bandwidth] could not read Docker stats for %s: %s",
                                 self.container_name, exc)
            return None

        networks = (stats or {}).get("networks") or {}
        if not networks:
            if not self._warned:
                self._warned = True
                self.log.warning(
                    "[bandwidth] Docker reports no network counters for container %s. "
                    "This happens when the experiment runs in the host network "
                    "namespace, where its traffic cannot be told apart from the rest "
                    "of the node's. Rate and transfer limits cannot be enforced for "
                    "this run; the collected-data limit still is.",
                    self.container_name)
            return None

        rx = sum(int(interface.get("rx_bytes") or 0) for interface in networks.values())
        tx = sum(int(interface.get("tx_bytes") or 0) for interface in networks.values())
        return {"rx_bytes": rx, "tx_bytes": tx}


class BandwidthGuard:
    """Samples one container against a node policy and stops it on a breach.

    :param policy: the node's stored policy, in any form
        :func:`common.bandwidth_policy.normalise` accepts.
    :param stats_reader: anything with ``read()`` as :class:`DockerStatsReader`.
    :param stop_container: called once, with no arguments, to end the
        measurement. It must not raise.
    :param artifact_dir: the directory the container collects data into, or
        ``None`` to leave the collected-data cap unmeasured.
    :param csv_path: where to append the per-second trace, or ``None``.
    :param now: callable returning the node's local time, used to decide which
        window applies. Local, not UTC: an owner's "09:00-17:00" means their
        own working day.
    :param waiter: how the loop waits between samples, as a callable taking
        seconds and returning True to stop. The default waits on the stop
        event, so a run that ends normally does not pay a last whole interval
        before its guard notices. Tests pass a waiter that drives a fake clock.
    """

    def __init__(self, policy, stats_reader, stop_container, log,
                 artifact_dir=None, csv_path=None, now=datetime.now,
                 monotonic_clock=monotonic, waiter=None):
        self.policy = bp.normalise(policy)
        self.stats_reader = stats_reader
        self.stop_container = stop_container
        self.log = log
        self.artifact_dir = artifact_dir
        self.csv_path = csv_path
        self._now = now
        self._monotonic = monotonic_clock

        self.interval = self.policy["enforcement"]["sample_interval_secs"]
        self._stop_event = threading.Event()
        self._wait = waiter if waiter is not None else self._stop_event.wait
        self._thread = None
        self._lock = threading.Lock()

        self.breach = None            # (reason_code, message) once stopped
        self.samples = 0
        self.network_measured = False
        self.download_bytes = None
        self.upload_bytes = None
        self.artifact_bytes = None
        self.peak_download_bps = 0
        self.peak_upload_bps = 0
        self._state = bp.new_state()
        self._csv_error_logged = False

    # ------------------------------------------------------------------
    @property
    def enforcing(self):
        """True when this policy would actually stop something."""
        return bp.is_enforced(self.policy)

    def limits_now(self):
        return bp.effective_limits(self.policy, self._now())

    def start(self):
        """Start sampling. Safe to call when the policy does nothing: it logs and returns."""
        limits = self.limits_now()
        if not bp.has_any_limit(self.policy):
            self.log.info("[bandwidth] node has no bandwidth policy; the run is not limited")
            return self
        if not limits["enforced"]:
            self.log.info("[bandwidth] node policy is advisory (enforcement off); "
                          "sampling for the record but the run will not be stopped")
        else:
            self.log.info("[bandwidth] enforcing node policy: interval=%ds grace=%ds "
                          "window=%s download_limit=%s upload_limit=%s "
                          "caps: down=%s up=%s total=%s collected=%s",
                          self.interval, limits["breach_grace_secs"],
                          limits["window_id"] or "none",
                          bp.format_rate(limits["download_bps"]),
                          bp.format_rate(limits["upload_bps"]),
                          bp.format_bytes(limits["max_download_bytes"]),
                          bp.format_bytes(limits["max_upload_bytes"]),
                          bp.format_bytes(limits["max_total_bytes"]),
                          bp.format_bytes(limits["max_artifact_bytes"]))
        self._write_csv_header()
        self._thread = threading.Thread(target=self._loop, name="bandwidth-guard", daemon=True)
        self._thread.start()
        return self

    def stop(self, timeout=10):
        """Stop sampling and wait for the thread to finish."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                self.log.warning("[bandwidth] guard thread did not stop within %ss", timeout)
        return self.breach

    def wait(self, timeout=None):
        """Block until the guard stops the run, or until ``timeout``. True if it did."""
        self._stop_event.wait(timeout)
        return self.breach is not None

    def breached(self):
        with self._lock:
            return self.breach

    # ------------------------------------------------------------------
    def _loop(self):
        baseline = self.stats_reader.read()
        previous = baseline
        last_time = self._monotonic()
        started = last_time
        artifact_bytes = None

        while not self._stop_event.is_set():
            if self._wait(self.interval):
                break

            current = self.stats_reader.read()
            now_mono = self._monotonic()
            interval = max(now_mono - last_time, 1e-6)
            last_time = now_mono

            download_bps = upload_bps = None
            if current is not None:
                if baseline is None:
                    # Counters appeared later than the container did; this
                    # sample is the baseline, not a spike from zero.
                    baseline = previous = current
                else:
                    if previous is not None:
                        download_bps = max(0.0, (current["rx_bytes"] - previous["rx_bytes"]) * 8.0 / interval)
                        upload_bps = max(0.0, (current["tx_bytes"] - previous["tx_bytes"]) * 8.0 / interval)
                    previous = current

            download_bytes = upload_bytes = None
            if baseline is not None and current is not None:
                download_bytes = max(0, current["rx_bytes"] - baseline["rx_bytes"])
                upload_bytes = max(0, current["tx_bytes"] - baseline["tx_bytes"])

            self.samples += 1
            if self.samples % ARTIFACT_SAMPLE_EVERY == 1 or self.artifact_dir is None:
                artifact_bytes = directory_size(self.artifact_dir)

            limits = self.limits_now()
            sample = {
                "download_bps": download_bps,
                "upload_bps": upload_bps,
                "download_bytes": download_bytes,
                "upload_bytes": upload_bytes,
                "artifact_bytes": artifact_bytes,
                "interval_secs": interval,
            }

            with self._lock:
                self.network_measured = self.network_measured or current is not None
                self.download_bytes = download_bytes
                self.upload_bytes = upload_bytes
                self.artifact_bytes = artifact_bytes
                if download_bps is not None:
                    self.peak_download_bps = max(self.peak_download_bps, download_bps)
                if upload_bps is not None:
                    self.peak_upload_bps = max(self.peak_upload_bps, upload_bps)

            self._append_csv(sample, limits, now_mono - started)

            verdict = bp.evaluate(limits, sample, self._state)
            if verdict is None:
                continue

            reason_code, message = verdict
            with self._lock:
                self.breach = (reason_code, message)
            self.log.warning("[bandwidth] STOPPING the measurement: %s (%s)", message, reason_code)
            try:
                self.stop_container()
            except Exception:
                self.log.exception("[bandwidth] could not stop the container after a policy breach")
            self._stop_event.set()
            break

    # ------------------------------------------------------------------
    def _write_csv_header(self):
        if not self.csv_path:
            return
        try:
            with open(self.csv_path, "w", newline="") as handle:
                csv.writer(handle).writerow(CSV_FIELDS)
        except OSError as exc:
            self.log.warning("[bandwidth] could not open the trace file %s: %s", self.csv_path, exc)
            self.csv_path = None

    def _append_csv(self, sample, limits, elapsed):
        if not self.csv_path:
            return
        row = [
            self._now().isoformat(timespec="seconds"),
            round(elapsed, 3),
            round(sample["interval_secs"], 3),
            sample["download_bytes"] if sample["download_bytes"] is not None else "",
            sample["upload_bytes"] if sample["upload_bytes"] is not None else "",
            int(sample["download_bps"]) if sample["download_bps"] is not None else "",
            int(sample["upload_bps"]) if sample["upload_bps"] is not None else "",
            sample["artifact_bytes"] if sample["artifact_bytes"] is not None else "",
            limits["window_id"] or "",
            limits["download_bps"] or "",
            limits["upload_bps"] or "",
        ]
        try:
            with open(self.csv_path, "a", newline="") as handle:
                csv.writer(handle).writerow(row)
        except OSError as exc:
            if not self._csv_error_logged:
                self._csv_error_logged = True
                self.log.warning("[bandwidth] could not append to the trace file %s: %s",
                                 self.csv_path, exc)

    # ------------------------------------------------------------------
    def summary(self):
        """``(label, value)`` pairs for the run report. Readable without this module."""
        with self._lock:
            breach = self.breach
            measured = self.network_measured
            download_bytes, upload_bytes = self.download_bytes, self.upload_bytes
            artifact_bytes = self.artifact_bytes
            peak_download, peak_upload = self.peak_download_bps, self.peak_upload_bps
            samples = self.samples

        limits = self.limits_now()
        rows = [
            ("Bandwidth policy", "enforced" if limits["enforced"]
             else ("advisory (enforcement off)" if bp.has_any_limit(self.policy) else "none")),
            ("Policy sample interval", "%ds" % self.interval),
            ("Samples taken", samples),
        ]
        if limits["window_id"]:
            rows.append(("Active limit window", limits["window_label"]))
        rows += [
            ("Download limit", bp.format_rate(limits["download_bps"])),
            ("Upload limit", bp.format_rate(limits["upload_bps"])),
            ("Per-run transfer cap", bp.format_bytes(limits["max_total_bytes"])),
            ("Per-run collected-data cap", bp.format_bytes(limits["max_artifact_bytes"])),
        ]
        if measured:
            rows += [
                ("Downloaded by the container", bp.format_bytes(download_bytes)),
                ("Uploaded by the container", bp.format_bytes(upload_bytes)),
                ("Peak download", bp.format_rate(peak_download)),
                ("Peak upload", bp.format_rate(peak_upload)),
            ]
        else:
            rows.append(("Container network counters",
                         "not available (host network namespace); rate and transfer "
                         "limits could not be enforced for this run"))
        rows.append(("Data collected", bp.format_bytes(artifact_bytes)))
        if breach:
            rows += [("Stopped by node policy", breach[0]), ("Policy stop detail", breach[1])]
        return rows
