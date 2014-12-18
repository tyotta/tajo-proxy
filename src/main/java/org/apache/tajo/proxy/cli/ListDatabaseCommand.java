package org.apache.tajo.proxy.cli;

import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

public class ListDatabaseCommand extends TajoProxyShellCommand {

  public ListDatabaseCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\l";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    for (String databaseName : client.getAllDatabaseNames()) {
      context.getOutput().println(databaseName);
    }
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "list all databases";
  }
}
