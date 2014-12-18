<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.proxy.*" %>
<%@ page import="org.apache.tajo.util.StringUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.jdbc.proxy.*" %>

<%
  TajoProxyServer tajoProxyServer =
      (TajoProxyServer) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  JSPContext tajoJSPContext = tajoProxyServer.getJSPContext();
  TajoProxyClient proxyClient = tajoJSPContext.getClient();

  String selectedUserId = request.getParameter("userId");
  String queryUserId = "";
  if (selectedUserId == null || selectedUserId.isEmpty() || "ALL".equals(selectedUserId)) {
    selectedUserId = "ALL";
  } else {
    queryUserId = selectedUserId;
  }

  String proxyOnlyParam = request.getParameter("proxyOnly");
  String proxyOnlyValue = "";
  boolean proxyOnly = false;
  if (proxyOnlyParam != null && !proxyOnlyParam.isEmpty() && "true".equals(proxyOnlyParam)) {
    proxyOnly = true;
    proxyOnlyValue = "checked";
  }
  List<QueryHistory> queries = proxyClient.listQueryHistory(queryUserId, proxyOnly);

  List<QueryHistory> runningQueries = new ArrayList<QueryHistory>();
  List<QueryHistory> finishedQueries = new ArrayList<QueryHistory>();

  for (QueryHistory eachQuery: queries) {
    if (TajoProxyClient.isQueryRunnning(eachQuery.getState())) {
      runningQueries.add(eachQuery);
    } else {
      finishedQueries.add(eachQuery);
    }
  }

  ProxyUserManageService userManager = tajoProxyServer.getClientRpcService().getUserManager();
  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Tajo-Proxy</title>
    <script src="/static/js/jquery.js" type="text/javascript"></script>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Proxy Server: <%=tajoProxyServer.getProxyServerName()%></h2>
  <hr/>
  <div>
    <form action='query.jsp' method='GET'>
      User: <select name="userId" width="190" style="width: 190px">
        <option value="ALL" <%=selectedUserId.equals("ALL") ? "selected" : ""%>>All</option>
        <%
          for (ProxyUser eachUser : userManager.getProxyUsers()) {
            if (selectedUserId.equals(eachUser.getUserId())) { %>
        <option value="<%=eachUser.getUserId()%>" selected><%=eachUser.getUserId()%>
            <%} else {%>
        <option value="<%=eachUser.getUserId()%>"><%=eachUser.getUserId()%></option>
        <%}
        }
        %>
      </select>
      &nbsp;&nbsp;&nbsp;&nbsp;Proxy Query: <input type="checkbox" name="proxyOnly" value="true" <%=proxyOnlyValue%>/>
      <input type="submit" value="Query"/>
    </form>
  </div>
  <hr/>
  <h3>Running Queries</h3>
<%
  if(runningQueries.isEmpty()) {
    out.write("No running queries");
  } else {
%>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Progress</th><th>Time</th><th>Status</th><th>External</th><th>sql</th></tr>
    <%
      for(QueryHistory eachQuery: runningQueries) {
        long time = System.currentTimeMillis() - eachQuery.getStartTime();
    %>
    <tr>
      <td><%=eachQuery.getQueryId()%></td>
      <td><%=eachQuery.getQueryMasterHost()%>:<%=eachQuery.getQueryMasterPort()%></td>
      <td><%=df.format(eachQuery.getStartTime())%></td>
      <td><%=(int)(eachQuery.getProgress() * 100.0f)%>%</td>
      <td><%=StringUtils.formatTime(time)%></td>
      <td><%=eachQuery.getState()%></td>
      <td><%=eachQuery.getQueryExternalParam() == null ? "-" : eachQuery.getQueryExternalParam().toString()%></td>
      <td><%=eachQuery.getQuery()%></td>
    </tr>
    <%
      }
    %>
  </table>
<%
  }
%>
  <p/>
  <hr/>
  <h3>Finished Queries</h3>
  <%
    if(finishedQueries.isEmpty()) {
      out.write("No finished queries");
    } else {
  %>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>Query Master</th><th>Started</th><th>Finished</th><th>Time</th><th>Status</th><th>External</th><th>sql</th></tr>
    <%
      for(QueryHistory eachQuery: finishedQueries) {
        long runTime = eachQuery.getFinishTime() > 0 ?
                eachQuery.getFinishTime() - eachQuery.getStartTime() : -1;
    %>
    <tr>
      <td><%=eachQuery.getQueryId()%></td>
      <td><%=eachQuery.getQueryMasterHost()%>:<%=eachQuery.getQueryMasterPort()%></td>
      <td><%=df.format(eachQuery.getStartTime())%></td>
      <td><%=eachQuery.getFinishTime() > 0 ? df.format(eachQuery.getFinishTime()) : "-"%></td>
      <td><%=runTime == -1 ? "-" : StringUtils.formatTime(runTime) %></td>
      <td><%=eachQuery.getState()%></td>
      <td><%=eachQuery.getQueryExternalParam() == null ? "-" : eachQuery.getQueryExternalParam().toString()%></td>
      <td><%=eachQuery.getQuery()%></td>
    </tr>
    <%
      }
    %>
  </table>
<%
  }
%>
</div>
</body>
</html>
