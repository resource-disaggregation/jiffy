# Run a shell command on all servers.
#
# Environment Variables
#
#   MMUX_SERVERS    File naming remote servers.
#     Default is ${MMUX_CONF_DIR}/servers.
#   MMUX_CONF_DIR  Alternate conf dir. Default is ${MMUX_HOME}/conf.
#   MMUX_HOST_SLEEP Seconds to sleep between spawning remote commands.
#   MMUX_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: servers.sh [--config <conf-dir>] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/mmux-config.sh"

# If the servers file is specified in the command line,
# then it takes precedence over the definition in
# mmux-env.sh. Save it here.
if [ -f "$MMUX_SERVERS" ]; then
  SERVERLIST=`cat "$MMUX_SERVERS"`
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
    export MMUX_CONF_DIR="$conf_dir"
  fi
  shift
fi

if [ "$SERVERLIST" = "" ]; then
  if [ "$MMUX_SERVERS" = "" ]; then
    if [ -f "${MMUX_CONF_DIR}/storage.hosts" ]; then
      SERVERLIST=`cat "${MMUX_CONF_DIR}/storage.hosts"`
    else
      SERVERLIST=localhost
    fi
  else
    SERVERLIST=`cat "${MMUX_SERVERS}"`
  fi
fi



# By default disable strict host key checking
if [ "$MMUX_SSH_OPTS" = "" ]; then
  MMUX_SSH_OPTS="-o StrictHostKeyChecking=no"
fi

for host in `echo "$SERVERLIST"|sed  "s/#.*$//;/^$/d"`; do
  if [ -n "${MMUX_SSH_FOREGROUND}" ]; then
    ssh $MMUX_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /"
  else
    ssh $MMUX_SSH_OPTS "$host" $"${@// /\\ }" \
      2>&1 | sed "s/^/$host: /" &
  fi
  if [ "$MMUX_HOST_SLEEP" != "" ]; then
    sleep $MMUX_HOST_SLEEP
  fi
done

wait
