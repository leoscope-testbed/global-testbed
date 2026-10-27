#!/usr/bin/env python3
"""Check whether the node can use the blob storage it has been given.

Run it inside the node container, so it sees the same orchestrator, the same
network and the same credentials the executor does. The image does not ship
``scripts/``, so copy it in first:

    docker cp scripts/check_blob_credentials.py \
        $(docker compose -f docker-compose-node.yaml ps -q node):/leotest/
    docker compose -f docker-compose-node.yaml exec node \
        python /leotest/check_blob_credentials.py \
            --nodeid <nodeid> --grpc-hostname <orchestrator> --grpc-port <port> \
            --access-token <token> [--jobid <jobid>] [--write]

It reports, separately:

* the orchestrator's global ``datastore.blob`` config -- the credential used for
  the cached config copies and as the fallback for artifact upload, and
* the ``cloud_config`` of one job, if ``--jobid`` is given -- the credential that
  job's artifacts are actually uploaded with.

A run can fail on either, so knowing which one storage rejects is the whole
question. Nothing is written to storage unless ``--write`` is passed.
"""

import argparse
import os
import sys
import uuid

# Importable whether it runs from the repository root or from /leotest after
# being copied into the node container.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import yaml
from google.protobuf.json_format import MessageToDict

from common import config as cfg
from common import job_events as je
from common.client import LeotestClient


def redact(connection_string):
    """Enough of a connection string to tell two of them apart, and no key."""
    if not connection_string:
        return "(empty)"
    parts = {}
    for chunk in connection_string.split(";"):
        key, _, value = chunk.partition("=")
        parts[key.strip().lower()] = value
    account = parts.get("accountname") or "(no AccountName)"
    key = parts.get("accountkey") or ""
    if key:
        secret = "AccountKey len=%d ending %r" % (len(key), key[-4:])
    elif "sharedaccesssignature" in parts:
        secret = "SAS token"
    else:
        secret = "(no AccountKey and no SAS)"
    return "AccountName=%s, %s" % (account, secret)


def probe(label, connection_string, container, write=False):
    """Try to use a credential. Returns True when storage accepted it."""
    print("\n%s" % label)
    print("  container : %s" % (container or "(empty)"))
    print("  credential: %s" % redact(connection_string))

    if not connection_string or not container:
        print("  RESULT    : NOT CONFIGURED — nothing to test")
        return False

    try:
        from common.utils import StorageDirectoryClient
        client = StorageDirectoryClient(connection_string, container)
    except Exception as exc:
        print("  RESULT    : UNUSABLE — the connection string itself was rejected")
        print("  error     : %s" % exc)
        return False

    try:
        client.check_blob_exists("leoscope-credential-check/does-not-exist")
        print("  RESULT    : OK for reads")
    except Exception as exc:
        reason = je.classify_storage_error(str(exc))
        print("  RESULT    : READ FAILED%s" % (" (%s)" % reason if reason else ""))
        print("  error     : %s" % exc)
        hint = je.storage_hint(reason)
        if hint:
            print("  hint      : %s" % hint)
        return False

    if not write:
        print("  note      : reads only; pass --write to test an upload too")
        return True

    blob = "leoscope-credential-check/%s.txt" % uuid.uuid4().hex
    try:
        client.client.upload_blob(name=blob, data=b"leoscope credential check\n",
                                  overwrite=True)
        print("  RESULT    : OK for writes (wrote %s)" % blob)
    except Exception as exc:
        reason = je.classify_storage_error(str(exc))
        print("  RESULT    : WRITE FAILED%s" % (" (%s)" % reason if reason else ""))
        print("  error     : %s" % exc)
        return False
    try:
        client.client.delete_blob(blob)
        print("  cleanup   : removed %s" % blob)
    except Exception as exc:
        print("  cleanup   : could not remove %s (%s) — delete it by hand" % (blob, exc))
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodeid", required=True)
    parser.add_argument("--grpc-hostname", default=cfg.GRPC_HOSTNAME)
    parser.add_argument("--grpc-port", type=int, default=cfg.GRPC_PORT)
    parser.add_argument("--access-token", default="")
    parser.add_argument("--jobid", help="also test this job's own cloud_config")
    parser.add_argument("--write", action="store_true",
                        help="also upload and delete a small test blob")
    args = parser.parse_args()

    client = LeotestClient(grpc_hostname=args.grpc_hostname, grpc_port=args.grpc_port,
                           userid=args.nodeid, access_token=args.access_token)

    print("orchestrator: %s:%d  node: %s" % (args.grpc_hostname, args.grpc_port, args.nodeid))

    try:
        blob_cfg = (MessageToDict(client.get_config())
                    .get("config", {}).get("datastore", {}).get("blob", {}))
    except Exception as exc:
        print("\nFAILED to read the orchestrator's global config: %s" % exc)
        return 2

    global_ok = probe("Orchestrator global config (datastore.blob)",
                      blob_cfg.get("connectionString", ""),
                      blob_cfg.get("container", ""),
                      write=args.write)
    print("  artifactPath: %s" % blob_cfg.get("artifactPath", "(empty)"))

    job_ok = None
    if args.jobid:
        try:
            record = MessageToDict(client.get_job_by_id(args.jobid))
        except Exception as exc:
            print("\nFAILED to read job %s: %s" % (args.jobid, exc))
            return 2
        if not record.get("exists"):
            print("\nJob %s does not exist on the orchestrator" % args.jobid)
            return 2
        cloud = (yaml.safe_load(record.get("config") or "") or {}).get("cloud_config") or {}
        connection_string = (cloud.get("connection_string") or "").strip()
        container = (cloud.get("container") or "").strip()
        if not connection_string and not container:
            print("\nJob %s has no cloud_config of its own; it uses the global config "
                  "above." % args.jobid)
        else:
            job_ok = probe("Job %s cloud_config (used to upload its artifacts)" % args.jobid,
                           connection_string or blob_cfg.get("connectionString", ""),
                           container or blob_cfg.get("container", ""),
                           write=args.write)

    print("\n--- summary ---")
    print("global config : %s" % ("OK" if global_ok else "NOT USABLE"))
    if job_ok is not None:
        print("job config    : %s" % ("OK" if job_ok else "NOT USABLE"))
    if not global_ok and job_ok:
        print("A run of this job will now go ahead and upload its artifacts with its own "
              "cloud_config; fix the global config so runs without one work too.")
    if not global_ok and job_ok is None:
        print("Every run on this node will fail at upload until the global config is fixed.")
    return 0 if (global_ok or job_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
