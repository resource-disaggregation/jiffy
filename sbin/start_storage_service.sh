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

if [ "$SERVICE_PORT" = "" ]; then
  SERVICE_PORT="9093"
fi

if [ "$MANAGEMENT_SERVICE_PORT" = "" ]; then
  MANAGEMENT_SERVICE_PORT="9094"
fi

if [ "$BLOCK_ADDRESS" = "" ]; then
  BLOCK_ADDRESS="127.0.0.1"
fi

if [ "$BLOCK_SERVICE_PORT" = "" ]; then
  BLOCK_SERVICE_PORT="9092"
fi

if [ "$NOTIFICATION_SERVICE_PORT" = "" ]; then
  MANAGEMENT_PERIOD_MS="9095"
fi

if [ "$CHAIN_PORT" = "" ]; then
  CHAIN_PORT="9096"
fi

if [ "$NUM_BLOCKS" = "" ]; then
  NUM_BLOCKS="1"
fi

$sbin/../build/storage/storaged --address $BIND_ADDRESS --service-port $SERVICE_PORT \
  --management-port $MANAGEMENT_SERVICE_PORT --block-address $BLOCK_ADDRESS --block-port $BLOCK_SERVICE_PORT \
  --notification-port $NOTIFICATION_SERVICE_PORT --chain-port $CHAIN_PORT --num-blocks $NUM_BLOCKS \
  2>$LOG_PATH/storage.stderr 1>$LOG_PATH/storage.stdout &
