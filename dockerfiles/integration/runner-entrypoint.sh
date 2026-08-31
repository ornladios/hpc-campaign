#!/bin/sh
set -eu

mkdir -p /tmp/hpc-campaign-cache
hpc_campaign connector -c /root/.config/hpc-campaign/hosts.yaml -p 30000 >/tmp/hpc-campaign-connector.log 2>&1 &

exec "$@"
