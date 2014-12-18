#!/usr/bin/env bash

# Start tajo-proxy daemons.  Run this on proxy node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-proxy-config.sh

# start the tajo proxy daemon
"$bin"/tajo-proxy-daemon.sh --config $TAJO_PROXY_CONF_DIR start proxy

if [ -f "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh" ]; then
  . "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh"
fi

"$bin/tajo-proxy-daemons.sh" cd "$TAJO_PROXY_HOME" \; "$bin/tajo-proxy-daemon.sh" start proxy