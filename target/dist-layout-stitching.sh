run() {
                                                echo "\$ ${@}"
                                                "${@}"
                                                res=$?
                                                if [ $res != 0 ]; then
                                                    echo
                                                    echo "Failed!"
                                                    echo
                                                    exit $res
                                                fi
                                            }

                                            ROOT=`cd /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/..;pwd`
                                            echo
                                            echo "Current directory `pwd`"
                                            echo
                                            run rm -rf tajo-proxy-0.9.1-SNAPSHOT
                                            run mkdir tajo-proxy-0.9.1-SNAPSHOT
                                            run cd tajo-proxy-0.9.1-SNAPSHOT
                                            run cp /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/tajo-proxy-0.9.1-SNAPSHOT.jar .
                                            run cp -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/lib .
                                            run rm /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/*.class
                                            run rm -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/catalog
                                            run rm -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/common
                                            run rm -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/ipc
                                            run rm -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/jdbc
                                            run rm -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/rpc
                                            run rm -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/classes/org/apache/tajo/util
                                            run cp -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/src/main/bin .
                                            run cp -r /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/src/main/conf .
                                            run chmod 755 ./bin/*
                                            run chmod 755 ./conf/*.sh

                                            run cd ..
                                            #run rm ./tajo-proxy-0.9.1-SNAPSHOT/lib/netty-3.2.4.Final.jar
                                            run mkdir ./jdbc-dist
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/tajo-proxy-0.9.1-SNAPSHOT.jar ./jdbc-dist/tajo-proxy-jdbc-0.9.1-SNAPSHOT.jar
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/commons-logging-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/commons-lang-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/commons-configuration-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/commons-cli-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/commons-io-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/guava-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/hadoop-annotations-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/hadoop-auth-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/hadoop-common-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/log4j-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/netty-3.6.6.Final.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/protobuf-java-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/slf4j-api-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/slf4j-log4j12-*.jar ./jdbc-dist/.
                                            run cp ./tajo-proxy-0.9.1-SNAPSHOT/lib/tajo-*.jar ./jdbc-dist/.

                                            echo
                                            echo "Tajo-Proxy dist layout available at: /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/tajo-proxy-0.9.1-SNAPSHOT"
                                            echo