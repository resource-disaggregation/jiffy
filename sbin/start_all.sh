#!/usr/bin/env bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

echo "Starting directory service..."
$sbin/start_directory_service.sh
sleep 1
echo "Starting storage services..."
$sbin/start_all_storage.sh
sleep 1
echo "Done"
