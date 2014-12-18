#!/usr/bin/env bash

# Run a shell command on all proxyserver hosts.
#
# Environment Variables
#
#   TAJO_PROXY_SERVERS    File naming remote hosts.
#     Default is ${TAJO_PROXY_CONF_DIR}/proxyservers.
#   TAJO_PROXY_CONF_DIR  Alternate conf dir. Default is ${TAJO_HOME}/conf.
#   TAJO_PROXY_SLAVE_SLEEP Seconds to sleep between spawning remote commands.
#   TAJO_PROXY_SSH_OPTS Options passed to ssh when running remote commands.
##

usage="Usage: tajo-proxy-daemons.sh command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-proxy-config.sh

# If the proxyservers file is specified in the command line,
# then it takes precedence over the definition in
# tajo-proxy-env.sh. Save it here.
HOSTLIST=$TAJO_PROXY_SERVERS

if [ -f "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh" ]; then
  . "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$TAJO_PROXY_SERVERS" = "" ]; then
    export HOSTLIST="${TAJO_PROXY_CONF_DIR}/proxyservers"
  else
    export HOSTLIST="${TAJO_PROXY_CONF_DIR}"
  fi
fi

for slave in `cat "$HOSTLIST"`; do
 ssh $TAJO_PROXY_SSH_OPTS $slave $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [ "$TAJO_PROXY_SLAVE_SLEEP" != "" ]; then
   sleep $TAJO_PROXY_SLAVE_SLEEP
 fi
done

wait
