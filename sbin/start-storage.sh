#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/mmux-config.sh"

LOG_PATH="$MMUX_PREFIX/log"
mkdir -p $LOG_PATH

$sbin/../build/storage/storaged -c $MMUX_CONF_DIR/mmux.conf \
  2>$LOG_PATH/mmux-storage.stderr 1>$LOG_PATH/mmux-storage.stdout &
