<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.proxy.ProxyClientRpcService.*" %>
<%@ page import="org.apache.tajo.proxy.*" %>
<%@ page import="org.apache.tajo.util.StringUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.jdbc.proxy.*" %>

<%
  TajoProxyServer tajoProxyServer =
      (TajoProxyServer) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");

  Collection<QueryProgressInfo> queryTasks = tajoProxyServer.getClientRpcService().getQuerySubmitTasks();

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
  <h3>Running Query Tasks</h3>
<%
  if(queryTasks.isEmpty()) {
    out.write("No running query tasks.");
  } else {
%>
  <div># Tasks: <%=queryTasks.size()%></div>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>QueryId</th><th>SessionId</th><th>UserId</th><th>Started</th><th>Progress</th><th>Time</th><th>Status</th><th>Last Touch Time</th></th><th>sql</th></tr>
    <%
      for(QueryProgressInfo eachQuery: queryTasks) {
        long time = System.currentTimeMillis() - eachQuery.getQueryStatus().getSubmitTime();
    %>
    <tr>
      <td><%=eachQuery.getQueryId()%></td>
      <td><%=eachQuery.getSessionId().getId()%></td>
      <td><%=eachQuery.getRealUserId()%></td>
      <td><%=df.format(eachQuery.getQueryStatus().getSubmitTime())%></td>
      <td><%=(int)(eachQuery.getQueryStatus().getProgress() * 100.0f)%>%</td>
      <td><%=StringUtils.formatTime(time)%></td>
      <td><%=eachQuery.getQueryStatus().getState()%></td>
      <td><%=df.format(eachQuery.getLastTouchTime())%></td>
      <td><%=eachQuery.getRealQuery()%></td>
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
