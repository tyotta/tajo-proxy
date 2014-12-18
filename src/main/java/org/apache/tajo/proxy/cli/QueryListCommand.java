package org.apache.tajo.proxy.cli;

import org.apache.commons.lang.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.ipc.ClientProtos.BriefQueryInfo;
import org.apache.tajo.proxy.QueryHistory;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;
import org.apache.tajo.util.JSPUtil;

import java.text.SimpleDateFormat;
import java.util.List;

public class QueryListCommand extends TajoProxyShellCommand {
  final static String DATE_FORMAT  = "yyyy-MM-dd HH:mm:ss";

  public QueryListCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\lq";
  }

  @Override
  public void invoke(String[] command) throws Exception {
    List<QueryHistory> queryList = client.listQueryHistory(client.getCurrentUser());
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
    StringBuilder builder = new StringBuilder();

    /* print title */
    builder.append(StringUtils.rightPad("QueryId", 21));
    builder.append(StringUtils.rightPad("State", 20));
    builder.append(StringUtils.rightPad("StartTime", 20));
    builder.append(StringUtils.rightPad("Duration", 20));
    builder.append(StringUtils.rightPad("Progress", 10));
    builder.append(StringUtils.rightPad("Query", 30)).append("\n");

    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 20), 21));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 19), 20));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 8), 10));
    builder.append(StringUtils.rightPad(StringUtils.repeat("-", 29), 30)).append("\n");
    context.getOutput().write(builder.toString());

    builder = new StringBuilder();
    for (QueryHistory queryInfo : queryList) {
      builder.append(StringUtils.rightPad(queryInfo.getQueryId(), 21));
      builder.append(StringUtils.rightPad(queryInfo.getState(), 20));
      builder.append(StringUtils.rightPad(df.format(queryInfo.getStartTime()), 20));
      builder.append(StringUtils.rightPad(JSPUtil.getElapsedTime(queryInfo.getStartTime(), queryInfo.getFinishTime()), 20));
      builder.append(StringUtils.rightPad(JSPUtil.percentFormat(queryInfo.getProgress()) + "%", 10));
      builder.append(StringUtils.abbreviate(queryInfo.getQuery(), 30)).append("\n");
    }
    context.getOutput().write(builder.toString());
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "list running queries";
  }
}
