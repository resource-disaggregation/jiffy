#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/em-config.sh"
. "$ELASTICMEM_PREFIX/sbin/load-em-env.sh"

LOG_PATH="$sbin/../log"
mkdir -p $LOG_PATH

if [ "$BIND_ADDRESS" = "" ]; then
  BIND_ADDRESS="0.0.0.0"
fi

if [ "$DIRECTORY_SERVICE_PORT" = "" ]; then
  DIRECTORY_SERVICE_PORT="9090"
fi

if [ "$LEASE_SERVICE_PORT" = "" ]; then
  LEASE_SERVICE_PORT="9091"
fi

if [ "$BLOCK_SERVICE_PORT" = "" ]; then
  BLOCK_SERVICE_PORT="9092"
fi

if [ "$STORAGE_TRACE_FILE" = "" ]; then
  STORAGE_TRACE_FILE="$sbin/../log/storage.trace"
fi

if [ "$LEASE_PERIOD_MS" = "" ]; then
  LEASE_PERIOD_MS="60000000"
fi

if [ "$GRACE_PERIOD_MS" = "" ]; then
  GRACE_PERIOD_MS="60000000"
fi

$sbin/../build/directory/directoryd --address $BIND_ADDRESS --service-port $DIRECTORY_SERVICE_PORT \
  --lease-port $LEASE_SERVICE_PORT --block-port $BLOCK_SERVICE_PORT --storage-trace-file $STORAGE_TRACE_FILE \
  --lease-period-ms $LEASE_PERIOD_MS --grace-period-ms $GRACE_PERIOD_MS 2>$LOG_PATH/directory.stderr 1>$LOG_PATH/directory.stdout &
