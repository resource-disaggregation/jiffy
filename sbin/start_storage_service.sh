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

${sbin}/../build/storage/storaged --address ${STORAGE_ADDRESS} --service-port ${STORAGE_SERVICE_PORT} \
  --management-port ${MANAGEMENT_SERVICE_PORT} --notification-port ${NOTIFICATION_SERVICE_PORT} \
  --chain-port ${CHAIN_PORT} --dir-address ${DIRECTORY_ADDRESS} --dir-port ${DIRECTORY_SERVICE_PORT} \
  --block-port ${BLOCK_SERVICE_PORT} --num-blocks ${NUM_BLOCKS} --block-capacity ${BLOCK_CAPACITY} \
  2>${LOG_PATH}/storage.stderr 1>${LOG_PATH}/storage.stdout &
