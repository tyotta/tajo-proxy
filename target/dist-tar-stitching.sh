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

                                            run tar czf tajo-proxy-0.9.1-SNAPSHOT.tar.gz tajo-proxy-0.9.1-SNAPSHOT
                                            echo
                                            echo "Tajo-Proxy dist tar available at: /Users/seungunchoe/Documents/source_code/tajo/tajo-proxy-CDH5.2.0/target/tajo-proxy-0.9.1-SNAPSHOT.tar.gz"
                                            echo