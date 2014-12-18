<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.proxy.*" %>
<%@ page import="org.apache.tajo.conf.*" %>
<%@ page import="java.util.Map" %>

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
  <table width="100%" border="1" class="border_table">
<%
  for(Map.Entry<String,String> entry: tajoConf) {
%>
    <tr><td width="200"><%=entry.getKey()%></td><td><%=entry.getValue()%></td>
<%
  }
%>
  </table>
</div>
</body>
</html>