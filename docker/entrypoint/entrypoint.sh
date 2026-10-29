#!/bin/sh
set -eu

umask 077

for required_dir in /run/ape-dts /var/cache/ape-dts-platform/artifacts /data/storage; do
  if [ ! -d "${required_dir}" ]; then
    echo "required runtime directory is missing: ${required_dir}" >&2
    exit 1
  fi

  if [ ! -w "${required_dir}" ]; then
    echo "required runtime directory is not writable: ${required_dir}" >&2
    exit 1
  fi
done

exec node /app/dist/main.js
