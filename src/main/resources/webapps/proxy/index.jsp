<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.conf.TajoConf" %>
<%@ page import="org.apache.tajo.proxy.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.conf.TajoConf.ConfVars" %>

<%
  TajoProxyServer tajoProxyServer =
          (TajoProxyServer) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  TajoConf tajoConf = (TajoConf)tajoProxyServer.getConfig();
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo-Proxy</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Proxy Server: <%=tajoProxyServer.getProxyServerName()%></h2>
  <hr/>
  <h3>Proxy Server Status</h3>
  <table border='0'>
    <tr><td width='150'>Version:</td><td><%=tajoProxyServer.getVersion()%></td></tr>
    <tr><td width='150'>Started:</td><td><%=new Date(tajoProxyServer.getStartTime())%></td></tr>
    <tr><td width='150'>Tajo Master Server:</td><td><%=tajoConf.getVar(ConfVars.TAJO_MASTER_CLIENT_RPC_ADDRESS)%></td></tr>
    <tr><td width='150'>Heap(Free/Total/Max): </td><td><%=Runtime.getRuntime().freeMemory()/1024/1024%> MB / <%=Runtime.getRuntime().totalMemory()/1024/1024%> MB / <%=Runtime.getRuntime().maxMemory()/1024/1024%> MB</td>
    <tr><td width='150'>Configuration:</td><td><a href='conf.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Environment:</td><td><a href='env.jsp'>detail...</a></td></tr>
    <tr><td width='150'>Threads:</td><td><a href='thread.jsp'>thread dump...</a></tr>
  </table>
  <hr/>
  <h3>Proxy Server Summary</h3>
  <table class="border_table">
    <tr><td witdh='150'># Query Tasks: </td><td width='300'><a href='querytask.jsp'><%=tajoProxyServer.getClientRpcService().getQuerySubmitTasks().size()%></a></td></tr>
    <tr><td witdh='150'># Sessions: </td><td><a href='clientsession.jsp'><%=tajoProxyServer.getClientRpcService().getTajoClientSessions().size()%></a></td></tr>
  </table>
  <%@ include file="footer.jsp"%>
</div>
</body>
</html>
