package org.apache.tajo.jdbc.proxy;

import org.junit.Test;

import java.net.InetSocketAddress;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.*;

public class TestTajoProxyJdbcConnection {
  @Test
  public void testTajoProxyJdbcConnction() throws Exception {
    Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();
    String connUri = "jdbc:tajo-proxy://host01:10000/testdb?standby_proxy=host02:10001,host03:10002";
    TajoConnection conn = (TajoConnection)DriverManager.getConnection(connUri);

    assertEquals("testdb", conn.getDatabaseName());
    TajoProxyClient client = (TajoProxyClient)conn.getTajoClient();
    assertNotNull(client);

    List<InetSocketAddress> servers = client.getProxyServers();

    assertEquals(3, servers.size());

    assertEquals("host01", servers.get(0).getHostName());
    assertEquals(10000, servers.get(0).getPort());
    assertEquals("host02", servers.get(1).getHostName());
    assertEquals(10001, servers.get(1).getPort());
    assertEquals("host03", servers.get(2).getHostName());
    assertEquals(10002, servers.get(2).getPort());
  }

  @Test
  public void testTajoProxyJdbcConnctionNoDatabase() throws Exception {
    Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();
    String connUri = "jdbc:tajo-proxy://host01:10000?standby_proxy=host02:10001,host03:10002";
    TajoConnection conn = (TajoConnection) DriverManager.getConnection(connUri);

    assertEquals("default", conn.getDatabaseName());
  }

  @Test
  public void testTajoProxyJdbcConnctionCaseByCase1() throws Exception {
    Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();
    String connUri = "jdbc:tajo-proxy://babokim-mbp.server.gruter.com:9999";
    TajoConnection conn = (TajoConnection) DriverManager.getConnection(connUri);

    assertEquals("default", conn.getDatabaseName());
    TajoProxyClient client = (TajoProxyClient)conn.getTajoClient();
    assertNotNull(client);

    List<InetSocketAddress> servers = client.getProxyServers();

    assertEquals(1, servers.size());

    assertEquals("babokim-mbp.server.gruter.com", servers.get(0).getHostName());
    assertEquals(9999, servers.get(0).getPort());
  }

  @Test
  public void testTajoProxyJdbcConnctionNoPort() throws Exception {
    try {
      Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();
      String connUri = "jdbc:tajo-proxy://host01/testdb?standby_proxy=host02:10001,host03:10002";
      TajoConnection conn = (TajoConnection) DriverManager.getConnection(connUri);

      fail("proxymaster server must have port");
    } catch (SQLException e) {
      assertTrue(e.getMessage() != null && e.getMessage().indexOf("Wrong tajo-proxy master") >= 0);
    }

    try {
      Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();
      String connUri = "jdbc:tajo-proxy://host01:10000/testdb?standby_proxy=host02,host03:10002";
      TajoConnection conn = (TajoConnection) DriverManager.getConnection(connUri);

      fail("proxystandby server must have port");
    } catch (SQLException e) {
      assertTrue(e.getMessage() != null && e.getMessage().indexOf("Wrong tajo-proxy standby") >= 0);
    }
  }

  @Test
  public void testTajoProxyJdbcConnctionCharEncoding() throws Exception {
    Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();
    String connUri = "jdbc:tajo-proxy://host01:10000?characterEncoding=EUC-KR";
    TajoConnection conn = (TajoConnection) DriverManager.getConnection(connUri);

    assertEquals("EUC-KR", conn.getCharacterEncoding());
  }
}
