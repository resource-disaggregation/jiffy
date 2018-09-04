#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/mmux-config.sh"

LOG_PATH="$MMUX_PREFIX/log"
mkdir -p $LOG_PATH

$sbin/../build/directory/directoryd -c $MMUX_CONF_DIR/mmux.conf \
  2>$LOG_PATH/mmux-directory.stderr 1>$LOG_PATH/mmux-directory.stdout &
