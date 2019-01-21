#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/jiffy-config.sh"

$sbin/start-directory.sh
sleep 1
$sbin/hosts.sh $sbin/start-storage.sh
