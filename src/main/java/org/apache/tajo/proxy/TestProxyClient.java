package org.apache.tajo.proxy;

import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.jdbc.proxy.TajoProxyClient;
import org.apache.tajo.util.TUtil;

import java.util.List;

public class TestProxyClient {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.out.println("Usage: java TestProxyClient <proxyserver> <user> <pwd>");
      return;
    }
    TajoProxyClient client = new TajoProxyClient(new TajoConf(), TUtil.newList(args[0].split(",")), "default");

    client.login(args[1], args[2]);

    System.out.println("=====================");
    System.out.println("client.listQueryHistory()");
    List<QueryHistory> queries = client.listQueryHistory("");
    for (QueryHistory eachQuery: queries) {
      System.out.println(eachQuery);
    }
    System.out.println("\n\n=====================");
    System.out.println("client.listQueryHistory(\"\", true)");
    queries = client.listQueryHistory("", true);
    for (QueryHistory eachQuery: queries) {
      System.out.println(eachQuery);
    }
    System.out.println("\n\n=====================");
    System.out.println("client.listQueryHistory(" + args[1] + ", true)");
    queries = client.listQueryHistory(args[1], true);
    for (QueryHistory eachQuery: queries) {
      System.out.println(eachQuery);
    }
    System.out.println("\n\n=====================");
    System.out.println("client.getQueryHsitory(" + queries.get(0).getQueryId() + ")");
    QueryHistory query = client.getQueryHsitory(queries.get(0).getQueryId());
    System.out.println(query.toString());
    System.out.println("=====================");

    client.close();
  }
}
