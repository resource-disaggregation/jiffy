#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/mmux-config.sh"

$sbin/hosts.sh $sbin/stop-storage.sh
$sbin/stop-directory.sh
