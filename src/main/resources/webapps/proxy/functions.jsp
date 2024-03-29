<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="java.util.*" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.jdbc.proxy.*" %>
<%@ page import="org.apache.tajo.proxy.*" %>
<%@ page import="org.apache.tajo.catalog.*" %>
<%@ page import="org.apache.hadoop.http.HtmlQuoting" %>
<%@ page import="org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto" %>
<%
  TajoProxyServer tajoProxyServer =
      (TajoProxyServer) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  JSPContext tajoJSPContext = tajoProxyServer.getJSPContext();
  TajoProxyClient proxyClient = tajoJSPContext.getClient();

  List<FunctionDescProto> functions = proxyClient.getFunctions("");
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
    <h3>Catalog</h3>
    <div>
        <div style='float:left; margin-right:10px'><a href='catalogview.jsp'>[Table]</a></div>
        <div style='float:left; margin-right:10px'><a href='functions.jsp'>[Function]</a></div>
        <div style='clear:both'></div>
    </div>
    <p/>
    <table border="1" class='border_table'>
        <tr><th width='5%'>Name</th><th width='20%'>Signature</th><th width="5%">Type</th><th width='40%'>Description</th><th>Example</th></tr>
<%
    for(FunctionDescProto eachFunctionProto: functions) {
        FunctionDesc eachFunction = new FunctionDesc(eachFunctionProto);
        String fullDecription = eachFunction.getDescription();
        if(eachFunction.getDetail() != null && !eachFunction.getDetail().isEmpty()) {
            fullDecription += "\n" + eachFunction.getDetail();
        }
%>
        <tr>
            <td><%=eachFunction.getSignature()%></td>
            <td><%=eachFunction.getHelpSignature()%></td>
            <td><%=eachFunction.getFuncType()%></td>
            <td><%=HtmlQuoting.quoteHtmlChars(fullDecription).replace("\n", "<br/>")%></td>
            <td><%=HtmlQuoting.quoteHtmlChars(eachFunction.getExample()).replace("\n", "<br/>")%></td>
        </tr>
<%
    }
%>
    </table>
</div>
</body>
</html>
