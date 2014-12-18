Tajo Proxy Server
------------------------------------------------------
How to install

1. source download
   git clone https://github.com/gruter/tajo-proxy.git
2. build
   mvn clean package -DskipTests -Pdist -Dtar
3. cp target/tajo-proxy-0.9.0-SNAPSHOT.tar.gz $INSTALL_HOME
4. cd $INSTALL_HOME
5. tar xzf tajo-proxy-0.9.0-SNAPSHOT.tar.gz
6. cd tajo-proxy-0.9.0-SNAPSHOT
6. vi conf/tajo-proxy-env.sh
   set JAVA_HOME, PID_DIR
7. start proxy server
   bin/tajo-proxy-daemon.sh start proxy <port>
8. stop proxy server
   bin/tajo-proxy-daemon.sh stop proxy
9. query test
   bin/tajo-proxy proxy-shell <proxy-server-host>:<port> -q "select * from test"

