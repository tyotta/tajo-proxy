package org.apache.tajo.proxy;

import java.sql.*;

public class TajoProxyShell {
  public static void printUsage() {
    System.out.println("Usage: java TajoProxyShell <proxy-server:port> <command> [options]");
    System.out.println(" command is one of");
    System.out.println("   -q <query>: run query");
  }

  public static void runQuery(String proxyServer, String query) throws Exception {
//    List<String> servers = new ArrayList<String>();
//    servers.add(proxyServer);
//    TajoProxyClient tajoProxyClient = new TajoProxyClient(new TajoConf(), servers, "default");

    Class.forName("org.apache.tajo.jdbc.proxy.TajoDriver").newInstance();

    String connUri = "jdbc:tajo-proxy://" + proxyServer;

    Connection conn = DriverManager.getConnection(connUri);
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = conn.createStatement();
      long startTime = System.currentTimeMillis();
      rs = stmt.executeQuery(query);
      long endTime = System.currentTimeMillis();

      ResultSetMetaData rsmd = rs.getMetaData();
      for (int i = 0; i < rsmd.getColumnCount(); i++) {
        System.out.print(rsmd.getColumnName(i + 1) + "\t");
      }
      System.out.println();
      System.out.println("--------------------------------------------------------------------------------");
      int recordCount = 0;
      while (rs.next()) {
        for (int i = 0; i < rsmd.getColumnCount(); i++) {
          System.out.print(rs.getString(i + 1) + "\t");
        }
        System.out.println();
        recordCount++;
      }
      System.out.println();
      System.out.println(recordCount + " selected (" + (endTime - startTime) + " ms)");
    } finally {
      if (rs != null) {
        rs.close();
      }

      if (stmt != null) {
        stmt.close();
      }

      conn.close();
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      printUsage();
      return;
    }

    if ("-q".equals(args[1])) {
      if (args.length < 3) {
        printUsage();
        return;
      }
      runQuery(args[0], args[2]);
    } else {
      printUsage();
    }
  }


}
