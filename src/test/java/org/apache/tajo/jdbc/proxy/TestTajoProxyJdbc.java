package org.apache.tajo.jdbc.proxy;

import org.apache.tajo.proxy.TajoProxyServer;
import org.apache.tajo.proxy.TajoProxyServerTestCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestTajoProxyJdbc {
  private static Connection conn;
  private static String connUri;
  private static TajoProxyServerTestCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = new TajoProxyServerTestCluster();
    cluster.startCluster();

    TajoProxyServer tajoProxyServer = cluster.getTajoProxyServer();
    Thread.sleep(5000);
//    TajoConf tajoConf = new TajoConf();
//    tajoConf.setVar(TajoConf.ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS, "127.0.0.1:26002");
//    TajoProxyServer tajoProxyServer = new TajoProxyServer(9999);
//    tajoProxyServer.init(tajoConf);
//    tajoProxyServer.start();

    InetSocketAddress tajoProxyServerAddress = tajoProxyServer.getClientRpcService().getBindAddress();

    Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();

    connUri = "jdbc:tajo-proxy://" + tajoProxyServerAddress.getHostName() + ":" + tajoProxyServerAddress.getPort();
    conn = DriverManager.getConnection(connUri);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(conn != null) {
      conn.close();
    }
    if (cluster != null) {
      cluster.stopCluster();
    }
  }

  @Test
  public void testStatement() throws Exception {
    runBasicStatementAndAssert(conn, 5, 0);
  }

  @Test
  public void testStatementMaxRow() throws Exception {
    runBasicStatementAndAssert(conn, 5, 2);
  }

  private void runBasicStatementAndAssert(Connection conn, int tableNumRows, int maxRow) throws Exception {
    Statement stmt = null;
    ResultSet res = null;
    try {
      String[] columnNames = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
          "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
          "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

      stmt = conn.createStatement();

      int expectedNumRows = tableNumRows;
      if (maxRow > 0) {
        expectedNumRows = maxRow;
        stmt.setMaxRows(maxRow);
      }
      res = stmt.executeQuery("select * from lineitem");

      ResultSetMetaData rsmd = res.getMetaData();
      assertEquals(columnNames.length, rsmd.getColumnCount());

      for (int i = 0; i < columnNames.length; i++) {
        assertEquals(columnNames[i], rsmd.getColumnName(i + 1));
      }

      List<String> exprectedRows = new ArrayList<String>();

      exprectedRows.add("1|1|7706|1|17.0|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|");
      exprectedRows.add("1|1|7311|2|36.0|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |");
      exprectedRows.add("2|2|1191|1|38.0|44694.46|0.0|0.05|N|O|1997-01-28|1997-01-14|1997-02-02|TAKE BACK RETURN|RAIL|ven requests. deposits breach a|");
      exprectedRows.add("3|2|1798|1|45.0|54058.05|0.06|0.0|R|F|1994-02-02|1994-01-04|1994-02-23|NONE|AIR|ongside of the furiously brave acco|");
      exprectedRows.add("3|3|6540|2|49.0|46796.47|0.1|0.0|R|F|1993-11-09|1993-12-20|1993-11-24|TAKE BACK RETURN|RAIL| unusual accounts. eve|");

      int rowCount = 0;
      while(res.next()) {
        String row = "";
        for(int i = 0; i < rsmd.getColumnCount(); i++) {
          row += (res.getObject(i + 1) + "|");
        }
        assertEquals(exprectedRows.get(rowCount), row);
        rowCount++;
      }

      assertEquals(expectedNumRows, rowCount);
    } finally {
      if(res != null) {
        res.close();
      }
      if(stmt != null) {
        stmt.close();
      }
    }
  }

  @Test
  public void testPreparedStatement() throws Exception {
    PreparedStatement stmt = null;
    ResultSet res = null;
    try {
      String[] columnNames = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
          "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
          "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

      String sql = "select * from lineitem where l_orderkey = ? and l_returnflag = ?";

      stmt = conn.prepareStatement(sql);

      stmt.setInt(1, 1);
      stmt.setString(2, "N");

      res = stmt.executeQuery();

      ResultSetMetaData rsmd = res.getMetaData();

      assertEquals(columnNames.length, rsmd.getColumnCount());

      for (int i = 0; i < columnNames.length; i++) {
        assertEquals(columnNames[i], rsmd.getColumnName(i + 1));
      }

      List<String> exprectedRows = new ArrayList<String>();

      exprectedRows.add("1|1|7706|1|17.0|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the|");
      exprectedRows.add("1|1|7311|2|36.0|45983.16|0.09|0.06|N|O|1996-04-12|1996-02-28|1996-04-20|TAKE BACK RETURN|MAIL|ly final dependencies: slyly bold |");

      int rowCount = 0;
      while(res.next()) {
        String row = "";
        for(int i = 0; i < rsmd.getColumnCount(); i++) {
          row += res.getObject(i + 1)  + "|";
        }
        assertEquals(exprectedRows.get(rowCount), row);
        rowCount++;
      }

      assertEquals(exprectedRows.size(), rowCount);
    } finally {
      if(res != null) {
        res.close();
      }
      if(stmt != null) {
        stmt.close();
      }
    }
  }

  @Test
  public void testDatabaseMetaDataGetTable() throws Exception {
    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = null;

    try {
      rs = dbmd.getTables(null, null, null, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();

      assertEquals(5, numCols);
      int numTables = 0;

      List<String> tableNames = Arrays.asList(
          new String[]{"customer", "empty_orders", "lineitem", "nation", "orders",
                      "part", "partsupp", "region", "supplier"});

      Collections.sort(tableNames);

      while(rs.next()) {
        assertEquals(tableNames.get(numTables), rs.getString("TABLE_NAME"));
        numTables++;
      }

      assertEquals(tableNames.size(), numTables);
    } finally {
      if(rs != null) {
        rs.close();
      }
    }
  }

  @Test
  public void testDatabaseMetaDataGetColumns() throws Exception {
    DatabaseMetaData dbmd = conn.getMetaData();

    ResultSet rs = null;

    try {
      String tableName = "lineitem";
      rs = dbmd.getColumns(null, null, tableName, null);

      ResultSetMetaData rsmd = rs.getMetaData();
      int numCols = rsmd.getColumnCount();

      assertEquals(22, numCols);
      int numColumns = 0;

      String[] columnNames = {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
          "l_quantity", "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
          "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"};

      while(rs.next()) {
        assertEquals(tableName, rs.getString("TABLE_NAME"));
        assertEquals(columnNames[numColumns], rs.getString("COLUMN_NAME"));
        //TODO assert type
        numColumns++;
      }

      assertEquals(columnNames.length, numColumns);
    } finally {
      if(rs != null) {
        rs.close();
      }
    }
  }

  @Test
  public void testMultipleConnections() throws Exception {
    Connection[] conns = new Connection[2];
    conns[0] = DriverManager.getConnection(connUri);
    conns[1] = DriverManager.getConnection(connUri);

    try {
      for(int i = 0; i < conns.length; i++) {
        runBasicStatementAndAssert(conns[i], 5, 0);
      }
    } finally {
      conns[0].close();
      conns[1].close();
    }
  }

  @Test
  public void testMultipleConnectionsSequentialClose() throws Exception {
    Connection[] conns = new Connection[2];
    conns[0] = DriverManager.getConnection(connUri);
    conns[1] = DriverManager.getConnection(connUri);

    try {
      for(int i = 0; i < conns.length; i++) {
        runBasicStatementAndAssert(conns[i], 5, 0);
        conns[i].close();
      }
    } finally {
      if(!conns[0].isClosed()) {
        conns[0].close();
      }
      if(!conns[1].isClosed()) {
        conns[1].close();
      }
    }
  }
}
