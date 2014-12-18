package org.apache.tajo.jdbc.proxy;

import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.proxy.ProxyClientRpcService.ResultSetHolder;
import org.apache.tajo.proxy.TajoProxyServer;
import org.junit.Test;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestTajoProxyClientRealCluster {
  @Test
  public void testAllFunction() throws Exception {
    TajoConf tajoConf = new TajoConf();
    tajoConf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, "127.0.0.1:26002");
    TajoProxyServer tajoProxyServer = new TajoProxyServer(9999);
    tajoProxyServer.init(tajoConf);
    tajoProxyServer.start();

    List<String> proxyServers = new ArrayList<String>();
    proxyServers.add(tajoProxyServer.getClientRpcService().getBindAddress().getHostName() + ":" +
        tajoProxyServer.getClientRpcService().getBindAddress().getPort());

    TajoProxyClient tajoProxyClient = new TajoProxyClient(tajoConf, proxyServers, TajoConstants.DEFAULT_DATABASE_NAME);

    System.out.println("====== getTableList, getTableDesc =======");
    List<String> tables = tajoProxyClient.getTableList(null);
    assertTrue(tables.size() > 0);

    TableDesc table1Desc = null;
    for(String eachTable: tables) {
      System.out.println(eachTable);
      TableDesc tableDesc = tajoProxyClient.getTableDesc(eachTable);
      assertNotNull(tableDesc);
      System.out.println(tableDesc.toString());

      if (eachTable.equals("table1")) {
        table1Desc = tableDesc;
      }
    }

    System.out.println("====== executeQueryAndGetResult =======");
    TajoResultSet rs = (TajoResultSet)tajoProxyClient.executeQueryAndGetResult("select * from table1");
    assertNotNull(rs);
    assertTrue(rs.getTotalRow() > 0);
    ResultSetMetaData rsmd = rs.getMetaData();
    assertEquals(table1Desc.getSchema().getColumns().size(), rsmd.getColumnCount());

    for (int i = 0; i < rsmd.getColumnCount(); i++) {
      assertEquals(table1Desc.getSchema().getColumn(i).getSimpleName(), rsmd.getColumnName(i + 1));
    }

    int rowCount = 0;
    while (rs.next()) {
      if (rowCount == 0) {
        for (int i = 0; i < rsmd.getColumnCount(); i++) {
          assertNotNull(rs.getObject(i + 1));
        }
      }
      rowCount++;
    }
    assertEquals(rs.getTotalRow(), rowCount);

    rs.close();

    //SubmitTask and ResultSetHolder must be removed
    assertNull(tajoProxyServer.getClientRpcService().getQuerySubmitTask(tajoProxyClient.getLastSessionId().toString() + rs.getQueryId().toString()));
    assertNull(tajoProxyServer.getClientRpcService().getResultSetHolder(
        ResultSetHolder.getKey(tajoProxyClient.getLastSessionId(), rs.getQueryId())));
  }
}
