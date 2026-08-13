"""
Centralised environment-variable configuration for all LEOScope components.

Every configurable value lives here. All other modules import from this module
instead of calling os.getenv() directly. Call log_config_summary() near startup
to emit a redacted snapshot of the active configuration for debugging.

Required variables (no default) raise RuntimeError at import time if missing.
Optional variables fall back to the documented default.
"""

import os
import logging

log = logging.getLogger(__name__)


def _req(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(
            f"[config] Required environment variable {key!r} is not set or is empty. "
            "Check your docker-compose environment: or .env file."
        )
    return val


def _opt(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _opt_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        log.error("[config] %s=%r is not a valid integer; using default %d", key, raw, default)
        return default


# ---------------------------------------------------------------------------
# gRPC / Orchestrator
# ---------------------------------------------------------------------------
GRPC_HOST_SERVICE_NAME = _opt("LEOSCOPE_GRPC_HOST_SERVICE_NAME", "localhost")  # hostname used in gRPC service discovery (DNS)
GRPC_HOSTNAME = _opt("LEOSCOPE_GRPC_HOSTNAME", "localhost")
GRPC_PORT = _opt_int("LEOSCOPE_GRPC_PORT", 50051)

# TLS certificates — server reads key+crt, clients read crt as trust root

PRIMARY_CERT_PATH = _opt("LEOSCOPE_PRIMARY_CERT_PATH", "certs/primary.crt")
SECONDARY_CERT_PATH = _opt("LEOSCOPE_SECONDARY_CERT_PATH", "certs/secondary.crt")

GRPC_CERT_PATH = _opt("LEOSCOPE_GRPC_CERT_PATH", "certs/server.crt")
GRPC_KEY_PATH = _opt("LEOSCOPE_GRPC_KEY_PATH", "certs/server.key")

# Override CN used for TLS hostname verification (clients set this to match the cert CN)
GRPC_TLS_TARGET_NAME_OVERRIDE = _opt("LEOSCOPE_GRPC_TLS_TARGET_NAME_OVERRIDE", "localhost")

GRPC_MAX_WORKERS = _opt_int("LEOSCOPE_GRPC_MAX_WORKERS", 10)
GRPC_TIMEOUT_SECS = _opt_int("LEOSCOPE_GRPC_TIMEOUT_SECS", 5)
GRPC_CONN_RETRY_NUM = _opt_int("LEOSCOPE_GRPC_CONN_RETRY_NUM", 3)
GRPC_CONN_RETRY_WAIT = _opt_int("LEOSCOPE_GRPC_CONN_RETRY_WAIT", 3)

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGO_HOST = _opt("LEOSCOPE_MONGO_HOST", "localhost")
MONGO_PORT = _opt_int("LEOSCOPE_MONGO_PORT", 27017)
MONGO_DB = _opt("LEOSCOPE_MONGO_DB", "leotest")

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------
ADMIN_ACCESS_TOKEN = _opt("LEOSCOPE_ADMIN_ACCESS_TOKEN", "leotest-access-token")
JWT_SECRET = _opt("LEOSCOPE_JWT_SECRET", "")
JWT_ALGO = _opt("LEOSCOPE_JWT_ALGO", "HS256")

# User that gets extra node-admin rights beyond normal role/ownership checks.
# May be empty/unset — when unset no special node-admin identity is active.
_raw_node_admin = _opt("LEOSCOPE_NODE_ADMIN_USERID", "")
NODE_ADMIN_USERID: str | None = _raw_node_admin.strip().lower() or None

# ---------------------------------------------------------------------------
# Node-local services
# ---------------------------------------------------------------------------
MEMCACHED_HOST = _opt("LEOSCOPE_MEMCACHED_HOST", "memcached")
MEMCACHED_PORT = _opt_int("LEOSCOPE_MEMCACHED_PORT", 11211)

REDIS_HOST = _opt("LEOSCOPE_REDIS_HOST", "redis")
REDIS_PORT = _opt_int("LEOSCOPE_REDIS_PORT", 6379)
REDIS_DB = _opt_int("LEOSCOPE_REDIS_DB", 0)

MQTT_HOST = _opt("LEOSCOPE_MQTT_HOST", "mqtt")
MQTT_PORT = _opt_int("LEOSCOPE_MQTT_PORT", 1883)

# ---------------------------------------------------------------------------
# Node executables and file paths
# ---------------------------------------------------------------------------
PYTHON_BIN = _opt("LEOSCOPE_PYTHON_BIN", "/usr/local/bin/python")
WORKDIR = _opt("LEOSCOPE_WORKDIR", "/home/leotest/")
ARTIFACTDIR = _opt("LEOSCOPE_ARTIFACTDIR", "/artifacts/")
EXECUTOR_CONFIG = _opt("LEOSCOPE_EXECUTOR_CONFIG", "/executor-config.yaml")
EXPERIMENT_CONFIGS_DIR = _opt("LEOSCOPE_EXPERIMENT_CONFIGS_DIR", "/leotest/experiment_configs")

# ---------------------------------------------------------------------------
# Starlink dish / telemetry
# ---------------------------------------------------------------------------
STARLINK_GRPC_TOOLS_PATH = _opt("LEOSCOPE_STARLINK_GRPC_TOOLS_PATH", "/leotest/starlink-grpc-tools")
STARLINK_DISH_HOST = _opt("LEOSCOPE_STARLINK_DISH_HOST", "192.168.100.1")
STARLINK_DISH_PORT = _opt_int("LEOSCOPE_STARLINK_DISH_PORT", 9200)

# ---------------------------------------------------------------------------
# Kernel service
# ---------------------------------------------------------------------------
KERNEL_SERVICE_HOST = _opt("LEOSCOPE_KERNEL_SERVICE_HOST", "leotest_kernel_service")
KERNEL_SERVICE_PORT = _opt_int("LEOSCOPE_KERNEL_SERVICE_PORT", 9000)
KERNEL_NET_NAME = _opt("LEOSCOPE_KERNEL_NET_NAME", "global-testbed_leotest-net")

# ---------------------------------------------------------------------------
# Azure Blob Storage (global fallback; experiments can override per-job)
# ---------------------------------------------------------------------------
AZURE_CONNECTION_STRING = _opt("LEOSCOPE_AZURE_CONNECTION_STRING", "")
AZURE_CONTAINER = _opt("LEOSCOPE_AZURE_CONTAINER", "leotest")
AZURE_ARTIFACT_PATH = _opt("LEOSCOPE_AZURE_ARTIFACT_PATH", "jobs/")

# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------
SCHEDULER_INTERVAL_SECS = _opt_int("LEOSCOPE_SCHEDULER_INTERVAL_SECS", 10)
# How far in the future (secs) to search when rescheduling a scavenged ATQ job
SCHEDULER_RESCHED_BUFFER_SECS = _opt_int("LEOSCOPE_SCHEDULER_RESCHED_BUFFER_SECS", 1800)

# ---------------------------------------------------------------------------
# Dashboard agent
# ---------------------------------------------------------------------------
DASHBOARD_CLIENT_NAME = _opt("LEOSCOPE_DASHBOARD_CLIENT_NAME", "")
DASHBOARD_UPLOAD_URL = _opt("LEOSCOPE_DASHBOARD_UPLOAD_URL", "")
# Starlink dish gRPC endpoint for the embedded speed test (dashboard agent)
# Falls back to 192.168.1.1:9000 on older dish firmware
DASHBOARD_STARLINK_GRPC_EP = _opt("LEOSCOPE_DASHBOARD_STARLINK_GRPC_EP", "192.168.100.1:9200")


def log_config_summary() -> None:
    """Emit a redacted snapshot of the active configuration. Call once at startup."""
    log.info("=" * 60)
    log.info("[config] LEOScope active configuration (secrets redacted)")
    log.info("[config] grpc_hostname=%s  grpc_port=%d", GRPC_HOSTNAME, GRPC_PORT)
    log.info("[config] grpc_host_service_name=%s", GRPC_HOST_SERVICE_NAME)
    log.info("[config] grpc_cert_path=%s  grpc_key_path=%s", GRPC_CERT_PATH, GRPC_KEY_PATH)
    log.info("[config] primary_cert_path=%s  secondary_cert_path=%s", PRIMARY_CERT_PATH, SECONDARY_CERT_PATH)
    log.info("[config] grpc_tls_target_name_override=%s", GRPC_TLS_TARGET_NAME_OVERRIDE)
    log.info("[config] grpc_max_workers=%d  grpc_timeout_secs=%d", GRPC_MAX_WORKERS, GRPC_TIMEOUT_SECS)
    log.info("[config] mongo_host=%s  mongo_port=%d  mongo_db=%s", MONGO_HOST, MONGO_PORT, MONGO_DB)
    log.info("[config] admin_access_token=<redacted>  jwt_algo=%s  jwt_secret=<redacted>", JWT_ALGO)
    log.info("[config] node_admin_userid=%s", NODE_ADMIN_USERID)
    log.info("[config] memcached=%s:%d  redis=%s:%d  mqtt=%s:%d",
             MEMCACHED_HOST, MEMCACHED_PORT, REDIS_HOST, REDIS_PORT, MQTT_HOST, MQTT_PORT)
    log.info("[config] python_bin=%s  workdir=%s  artifactdir=%s", PYTHON_BIN, WORKDIR, ARTIFACTDIR)
    log.info("[config] executor_config=%s  experiment_configs_dir=%s", EXECUTOR_CONFIG, EXPERIMENT_CONFIGS_DIR)
    log.info("[config] starlink_grpc_tools_path=%s  dish=%s:%d",
             STARLINK_GRPC_TOOLS_PATH, STARLINK_DISH_HOST, STARLINK_DISH_PORT)
    log.info("[config] kernel_service=%s:%d  net=%s",
             KERNEL_SERVICE_HOST, KERNEL_SERVICE_PORT, KERNEL_NET_NAME)
    log.info("[config] azure_container=%s  azure_artifact_path=%s  connection_string=<redacted>",
             AZURE_CONTAINER, AZURE_ARTIFACT_PATH)
    log.info("[config] scheduler_interval=%ds  resched_buffer=%ds",
             SCHEDULER_INTERVAL_SECS, SCHEDULER_RESCHED_BUFFER_SECS)
    log.info("=" * 60)
