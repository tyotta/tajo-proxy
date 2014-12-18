<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.proxy.*" %>

<%
  TajoProxyServer tajoProxyServer =
      (TajoProxyServer) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
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
  <h3>Thread Dump</h3>
  <pre><%tajoProxyServer.dumpThread(out);%></pre>
</div>
</body>
</html>
