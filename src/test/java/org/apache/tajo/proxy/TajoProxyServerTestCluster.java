
package org.apache.tajo.proxy;

import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;

import java.io.IOException;

public class TajoProxyServerTestCluster {
  private TajoTestingCluster cluster;
  private TajoProxyServer proxyServer;
  
  public void startCluster() {
    TpchTestBase testBase = TpchTestBase.getInstance();
    cluster = testBase.getTestingCluster();
    proxyServer = new TajoProxyServer(9999);
    proxyServer.init(cluster.getConfiguration());
    proxyServer.start();
  }
  
  public void stopCluster() {
    try {
      cluster.shutdownMiniCluster();
    } catch (IOException e) {
      e.printStackTrace();
    }
    
    proxyServer.stop();
  }
  
  public TajoProxyServer getTajoProxyServer() {
    return proxyServer;
  }
}
