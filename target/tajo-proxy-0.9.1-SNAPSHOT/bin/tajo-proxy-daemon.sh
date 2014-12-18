#!/usr/bin/env bash

# Runs a Tajo-proxy command as a daemon.
#
# Environment Variables
#
#   TAJO_PROXY_CONF_DIR  Alternate conf dir. Default is ${TAJO_PROXY_HOME}/conf.
#   TAJO_PROXY_LOG_DIR   Where log files are stored.  PWD by default.
#   TAJO_PROXY_PID_DIR   The pid files are stored. /tmp by default.
#   TAJO_PROXY_IDENT_STRING   A string representing this instance of tajo. $USER by default
#   TAJO_PROXY_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: tajo-proxy-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <tajo-proxy-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-proxy-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

tajo_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh" ]; then
  . "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh"
fi

if [ "$TAJO_PROXY_IDENT_STRING" = "" ]; then
  export TAJO_PROXY_IDENT_STRING="$USER"
fi

# get log directory
if [ "$TAJO_PROXY_LOG_DIR" = "" ]; then
  export TAJO_PROXY_LOG_DIR="$TAJO_PROXY_HOME/logs"
fi
mkdir -p "$TAJO_PROXY_LOG_DIR"
chown $TAJO_PROXY_IDENT_STRING $TAJO_PROXY_LOG_DIR

if [ "$TAJO_PROXY_PID_DIR" = "" ]; then
  TAJO_PROXY_PID_DIR=/tmp
fi

# some variables
export TAJO_PROXY_LOGFILE=tajo-$TAJO_PROXY_IDENT_STRING-$command-$HOSTNAME.log
export TAJO_PROXY_ROOT_LOGGER="INFO,DRFA"
log=$TAJO_PROXY_LOG_DIR/tajo-$TAJO_PROXY_IDENT_STRING-$command-$HOSTNAME.out
pid=$TAJO_PROXY_PID_DIR/tajo-$TAJO_PROXY_IDENT_STRING-$command.pid

# Set default scheduling priority
if [ "$TAJO_PROXY_NICENESS" = "" ]; then
    export TAJO_PROXY_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$TAJO_PROXY_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    tajo_rotate_log $log
    echo starting $command, logging to $log
    cd "$TAJO_PROXY_HOME"
    nohup nice -n $TAJO_PROXY_NICENESS "$TAJO_PROXY_HOME"/bin/tajo-proxy --config $TAJO_PROXY_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


