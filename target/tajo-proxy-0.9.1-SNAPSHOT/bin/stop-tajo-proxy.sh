#!/usr/bin/env bash

# Stop tajo proxy daemons.  Run this on proxy node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/tajo-proxy-config.sh

"$bin"/tajo-proxy-daemon.sh --config $TAJO_PROXY_CONF_DIR stop proxy

if [ -f "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh" ]; then
  . "${TAJO_PROXY_CONF_DIR}/tajo-proxy-env.sh"
fi

"$bin/tajo-proxy-daemons.sh" cd "$TAJO_PROXY_HOME" \; "$bin/tajo-proxy-daemon.sh" stop proxy
