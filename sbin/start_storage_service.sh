#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/em-config.sh"
. "$ELASTICMEM_PREFIX/sbin/load-em-env.sh"

LOG_PATH="$sbin/../log"
mkdir -p ${LOG_PATH}

if [ "$STORAGE_ADDRESS" = "" ]; then
  STORAGE_ADDRESS="0.0.0.0"
fi

if [ "$STORAGE_SERVICE_PORT" = "" ]; then
  STORAGE_SERVICE_PORT="9093"
fi

if [ "$MANAGEMENT_SERVICE_PORT" = "" ]; then
  MANAGEMENT_SERVICE_PORT="9094"
fi

if [ "$DIRECTORY_ADDRESS" = "" ]; then
  DIRECTORY_ADDRESS="127.0.0.1"
fi

if [ "$DIRECTORY_SERVICE_PORT" = "" ]; then
  DIRECTORY_SERVICE_PORT="9090"
fi

if [ "$BLOCK_SERVICE_PORT" = "" ]; then
  BLOCK_SERVICE_PORT="9092"
fi

if [ "$NOTIFICATION_SERVICE_PORT" = "" ]; then
  NOTIFICATION_SERVICE_PORT="9095"
fi

if [ "$CHAIN_PORT" = "" ]; then
  CHAIN_PORT="9096"
fi

if [ "$NUM_BLOCKS" = "" ]; then
  NUM_BLOCKS="1"
fi

if [ "$BLOCK_CAPACITY" = "" ]; then
  BLOCK_CAPACITY="134217728"
fi

if [ "$CAPACITY_THRESHOLD_LO" = "" ]; then
  CAPACITY_THRESHOLD_LO="0.25"
fi

if [ "$CAPACITY_THRESHOLD_HI" = "" ]; then
  CAPACITY_THRESHOLD_HI="0.75"
fi

if [ "$NON_BLOCKING" = "" ]; then
  NON_BLOCKING="false"
fi

if [ "$NON_BLOCKING" = "true" ]; then
  NB_FLAG="--non-blocking"
else
  NB_FLAG=""
fi

if [ "$NUM_IO_THREADS" = "" ]; then
  NUM_IO_THREADS="1"
fi

if [ "$NUM_PROC_THREADS" = "" ]; then
  NUM_PROC_THREADS="HARDWARE_CONCURRENCY"
fi

if [ "$STORAGE_TRACE_FILE" = "" ]; then
  STORAGE_TRACE_FILE="$sbin/../log/storage.trace"
fi

${sbin}/../build/storage/storaged --address ${STORAGE_ADDRESS} --service-port ${STORAGE_SERVICE_PORT} \
  --management-port ${MANAGEMENT_SERVICE_PORT} --notification-port ${NOTIFICATION_SERVICE_PORT} \
  --chain-port ${CHAIN_PORT} --dir-address ${DIRECTORY_ADDRESS} --dir-port ${DIRECTORY_SERVICE_PORT} \
  --block-port ${BLOCK_SERVICE_PORT} --num-blocks ${NUM_BLOCKS} --block-capacity ${BLOCK_CAPACITY} \
  --capacity-threshold-lo ${CAPACITY_THRESHOLD_LO} --capacity-threshold-hi ${CAPACITY_THRESHOLD_HI} ${NB_FLAG} \
  --io-threads ${NUM_IO_THREADS} --proc-threads ${NUM_PROC_THREADS} --storage-trace-file ${STORAGE_TRACE_FILE} \
  2>${LOG_PATH}/storage.stderr 1>${LOG_PATH}/storage.stdout &
