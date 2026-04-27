#!/usr/bin/env bash
set -euo pipefail

DATABASE="${DATABASE:-leotest}"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-}"
TARGET_CONTAINER="${TARGET_CONTAINER:-}"
SOURCE_MONGO_URI="${SOURCE_MONGO_URI:-mongodb://127.0.0.1:27017/${DATABASE}}"
TARGET_MONGO_URI="${TARGET_MONGO_URI:-mongodb://127.0.0.1:27017/${DATABASE}}"

find_source_container() {
  if [[ -n "${SOURCE_CONTAINER}" ]]; then
    echo "${SOURCE_CONTAINER}"
    return
  fi

  docker ps --format '{{.Names}}' |
    awk '
      $0 == "datastore" { print; found=1; exit }
      $0 !~ /redesign/ && $0 ~ /(^|[-_])datastore([-_]|$)/ { print; found=1; exit }
      END { if (!found) exit 1 }
    '
}

find_target_container() {
  if [[ -n "${TARGET_CONTAINER}" ]]; then
    echo "${TARGET_CONTAINER}"
    return
  fi

  docker ps --format '{{.Names}}' |
    awk '
      $0 == "datastore_redesign" { print; found=1; exit }
      $0 ~ /(^|[-_])datastore_redesign([-_]|$)/ { print; found=1; exit }
      END { if (!found) exit 1 }
    '
}

SOURCE_CONTAINER="$(find_source_container || true)"
TARGET_CONTAINER="$(find_target_container || true)"

if [[ -z "${SOURCE_CONTAINER}" ]]; then
  echo "Could not find the source production Mongo container." >&2
  echo "Set SOURCE_CONTAINER to the old Mongo container name, for example:" >&2
  echo "  SOURCE_CONTAINER=production_datastore_1 TARGET_CONTAINER=${TARGET_CONTAINER:-datastore_redesign} $0" >&2
  exit 1
fi

if [[ -z "${TARGET_CONTAINER}" ]]; then
  echo "Could not find the target redesign Mongo container." >&2
  echo "Start datastore_redesign first, or set TARGET_CONTAINER explicitly." >&2
  exit 1
fi

echo "Source container: ${SOURCE_CONTAINER}"
echo "Target container: ${TARGET_CONTAINER}"
echo "Source Mongo URI inside source container: ${SOURCE_MONGO_URI}"
echo "Target Mongo URI inside target container: ${TARGET_MONGO_URI}"
echo
echo "This will drop and replace data only in the target redesign Mongo database."
echo "Production/source Mongo is read-only for this operation."
echo

docker exec "${SOURCE_CONTAINER}" sh -lc 'command -v mongodump >/dev/null'
docker exec "${TARGET_CONTAINER}" sh -lc 'command -v mongorestore >/dev/null'

docker exec "${SOURCE_CONTAINER}" \
  sh -lc 'mongodump --uri="$1" --archive --gzip' \
  _ "${SOURCE_MONGO_URI}" |
docker exec -i "${TARGET_CONTAINER}" \
  sh -lc 'mongorestore --uri="$1" --archive --gzip --drop' \
  _ "${TARGET_MONGO_URI}"

echo
echo "Mongo copy complete."
