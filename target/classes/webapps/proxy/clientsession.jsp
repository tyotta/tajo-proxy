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

  Collection<TajoClientHolder> sessions = tajoProxyServer.getClientRpcService().getTajoClientSessions();
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
  <h3>Sessions</h3>
  <%
    if(sessions.isEmpty()) {
      out.write("No sessions.");
    } else {
  %>
  <div># Sessions: <%=sessions.size()%></div>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th>SessionID</th><th>Last Touch Time</th><th>UserId</th></tr>
    <%
      for(TajoClientHolder eachSession: sessions) {
        String sessionId = eachSession.getTajoClient().getSessionId().getId();
        ProxyUser user = userManager.getSessionUser(sessionId);
    %>
    <tr>
      <td><%=sessionId%></td>
      <td><%=df.format(eachSession.getLastTouchTime())%></td>
      <td><%=(user == null) ? "null" : user.getUserId()%></td>
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
