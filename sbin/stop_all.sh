#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

$sbin/stop_all_storage.sh
$sbin/stop_directory_service.sh
