#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/jiffy-config.sh"

LOG_PATH="$JIFFY_PREFIX/log"
mkdir -p $LOG_PATH

$sbin/../build/storage/storaged -c $JIFFY_CONF_DIR/jiffy.conf \
  2>$LOG_PATH/jiffy-storage.stderr 1>$LOG_PATH/jiffy-storage.stdout &
