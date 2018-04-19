# Run a shell command on all servers.
#
# Environment Variables
#
#   ELASTICMEM_SERVERS    File naming remote servers.
#     Default is ${ELASTICMEM_CONF_DIR}/servers.
#   ELASTICMEM_CONF_DIR  Alternate conf dir. Default is ${ELASTICMEM_HOME}/conf.
#   ELASTICMEM_HOST_SLEEP Seconds to sleep between spawning remote commands.
#   ELASTICMEM_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: servers.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

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



# By default disable strict host key checking
if [ "$ELASTICMEM_SSH_OPTS" = "" ]; then
  ELASTICMEM_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for host in `echo "$SERVERLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${ELASTICMEM_SSH_FOREGROUND}" ]; then
    ssh $ELASTICMEM_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /"
  else
    ssh $ELASTICMEM_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /" &
  fi
  if [ "$ELASTICMEM_HOST_SLEEP" != "" ]; then
    sleep $ELASTICMEM_HOST_SLEEP
  fi
done

wait
