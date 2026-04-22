#!/usr/bin/env bash
set -euo pipefail

DATABASE="${DATABASE:-leotest}"
COMPOSE_PROJECT="${COMPOSE_PROJECT:-global-testbed}"
NETWORK="${NETWORK:-${COMPOSE_PROJECT}_default}"
TOOLS_IMAGE="${TOOLS_IMAGE:-mongodb/mongodb-database-tools:100.11.0}"
SOURCE_MONGO_URI="${SOURCE_MONGO_URI:-mongodb://datastore:27017/${DATABASE}}"
TARGET_MONGO_URI="${TARGET_MONGO_URI:-mongodb://datastore_redesign:27017/${DATABASE}}"
BACKUP_DIR="${BACKUP_DIR:-$(mktemp -d)}"
ARCHIVE_PATH="/backup/${DATABASE}.archive.gz"

cleanup() {
  if [[ -z "${KEEP_BACKUP:-}" && -d "${BACKUP_DIR}" ]]; then
    rm -rf "${BACKUP_DIR}"
  fi
}
trap cleanup EXIT

echo "Source Mongo: ${SOURCE_MONGO_URI}"
echo "Target Mongo: ${TARGET_MONGO_URI}"
echo "Docker network: ${NETWORK}"
echo "Backup dir: ${BACKUP_DIR}"
echo
echo "This will drop and replace data only in the target redesign Mongo database."
echo "Production/source Mongo is read-only for this operation."
echo

docker run --rm \
  --network "${NETWORK}" \
  -v "${BACKUP_DIR}:/backup" \
  "${TOOLS_IMAGE}" \
  mongodump \
    --uri="${SOURCE_MONGO_URI}" \
    --archive="${ARCHIVE_PATH}" \
    --gzip

docker run --rm \
  --network "${NETWORK}" \
  -v "${BACKUP_DIR}:/backup" \
  "${TOOLS_IMAGE}" \
  mongorestore \
    --uri="${TARGET_MONGO_URI}" \
    --archive="${ARCHIVE_PATH}" \
    --gzip \
    --drop

echo
echo "Mongo copy complete."
