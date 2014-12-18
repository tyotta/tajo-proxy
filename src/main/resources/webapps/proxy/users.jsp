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

  ProxyUserManageService userManager = tajoProxyServer.getClientRpcService().getUserManager();
  Collection<ProxyUser> users = userManager.getProxyUsers();
  Collection<ProxyGroup> groups = userManager.getProxyGroups();
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
  <h3>Users</h3>
  <%
    if(users.isEmpty()) {
      out.write("No users.");
    } else {
  %>
  <div># Users: <%=users.size()%></div>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th width="200">UserId</th><th>Group</th></tr>
    <%
      for(ProxyUser eachUser: users) {
    %>
    <tr>
      <td><%=eachUser.getUserId()%></td>
      <td><%=eachUser.toGroupString()%></td>
    </tr>
    <%
      }
    %>
  </table>
  <%
    }
  %>
  <hr/>
  <h3>Groups</h3>
  <%
    if(groups.isEmpty()) {
      out.write("No groups.");
    } else {
  %>
  <div># Groups: <%=groups.size()%></div>
  <table width="100%" border="1" class='border_table'>
    <tr></tr><th width="200">Name</th><th>Users</th><th>Tables</th></tr>
    <%
      for(ProxyGroup eachGroup: groups) {
        String userList = "";
        String prefix = "";
        Set<String> groupUsers = userManager.getGroupUsers(eachGroup.getGroupName());
        if (groupUsers != null) {
          for (String eachUser: groupUsers) {
            userList += prefix + eachUser;
            prefix = ", ";
          }
        }
        prefix = "";
        String tableList = "";
        for (String eachTable: eachGroup.getTables()) {
          tableList += prefix + eachTable;
          prefix = "<br/>";
        }
    %>
    <tr>
      <td><%=eachGroup.getGroupName()%></td>
      <td><%=userList%></td>
      <td><%=tableList%></td>
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