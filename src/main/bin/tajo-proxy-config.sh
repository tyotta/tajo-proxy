# included in all the tajo-proxy scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the Tajo installation
export TAJO_PROXY_HOME=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      TAJO_PROXY_CONF_DIR=$confdir
    fi
fi
 
# Allow alternate conf dir location.
TAJO_PROXY_CONF_DIR="${TAJO_PROXY_CONF_DIR:-$TAJO_PROXY_HOME/conf}"

#check to see it is specified whether to use the proxyservers or the
# proxyservers file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        proxyserverfile=$1
        shift
        export TAJO_PROXY_SERVERS="${TAJO_CONF_DIR}/$proxyserverfile"
    fi
fi