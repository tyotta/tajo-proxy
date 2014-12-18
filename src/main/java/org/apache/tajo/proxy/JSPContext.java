package org.apache.tajo.proxy;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.proxy.TajoProxyClient;
import org.apache.tajo.util.TUtil;

import java.io.IOException;

public class JSPContext {
  private TajoProxyClient tajoProxyClient;
  private TajoProxyServer proxyServer;

  public JSPContext(TajoProxyServer proxyServer) {
    this.proxyServer = proxyServer;
  }

  public void init() throws Exception {
    tajoProxyClient = new TajoProxyClient(
        (TajoConf)proxyServer.getConfig(),
        TUtil.newList(proxyServer.getProxyServerName()),
        null);

    ProxyUser admin = proxyServer.getClientRpcService().getUserManager().getProxyUser("admin");
    if (admin == null) {
      throw new IOException("No admin user.");
    }
    tajoProxyClient.login("admin", admin.getPassword(), true);
  }

  public void close() throws Exception {
    if (tajoProxyClient != null) {
      tajoProxyClient.close();
    }
  }

  public TajoProxyClient getClient() {
    return tajoProxyClient;
  }
}
