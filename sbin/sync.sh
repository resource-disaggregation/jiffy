#!/bin/bash

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/em-config.sh"

# If the servers file is specified in the command line,
# then it takes precedence over the definition in
# em-env.sh. Save it here.
if [ -f "$ELASTICMEM_SERVERS" ]; then
  SERVERLIST=`cat "$ELASTICMEM_SERVERS"`
fi

# Check if --config is passed as an argument. It is an optional parameter.
# Exit if the argument is not a directory.
if [ "$1" == "--config" ]
then
  shift
  conf_dir="$1"
  if [ ! -d "$conf_dir" ]
  then
    echo "ERROR : $conf_dir is not a directory"
    echo $usage
    exit 1
  else
    export ELASTICMEM_CONF_DIR="$conf_dir"
  fi
  shift
fi

. "$ELASTICMEM_PREFIX/sbin/load-em-env.sh"

if [ "$SERVERLIST" = "" ]; then
  if [ "$ELASTICMEM_SERVERS" = "" ]; then
    if [ -f "${ELASTICMEM_CONF_DIR}/hosts" ]; then
      SERVERLIST=`cat "${ELASTICMEM_CONF_DIR}/hosts"`
    else
      SERVERLIST=localhost
    fi
  else
    SERVERLIST=`cat "${ELASTICMEM_SERVERS}"`
  fi
fi

DELETE_FLAG=""

usage() {
  echo "Usage: copy-dir [--delete] <dir>"
  exit 1
}

while :
do
  case $1 in
    --delete)
      DELETE_FLAG="--delete"
      shift
      ;;
    -*)
      echo "ERROR: Unknown option: $1" >&2
      usage
      ;;
    *) # End of options
      break
      ;;
  esac
done

if [[ "$#" != "1" ]] ; then
  usage
fi

if [[ ! -e "$1" ]] ; then
  echo "File or directory $1 doesn't exist!"
  exit 1
fi

DIR=`readlink -f "$1"`
DIR=`echo "$DIR"|sed 's@/$@@'`
DEST=`dirname "$DIR"`

SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=5"

echo "RSYNC'ing $DIR to servers..."
for server in $SERVERLIST; do
    echo $server
    rsync -e "ssh $SSH_OPTS" -az $DELETE_FLAG "$DIR" "$server:$DEST" & sleep 0.5
done
wait
