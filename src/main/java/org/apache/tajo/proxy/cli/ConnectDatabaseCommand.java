package org.apache.tajo.proxy.cli;

import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

public class ConnectDatabaseCommand extends TajoProxyShellCommand {

  public ConnectDatabaseCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\c";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 1) {
      context.getOutput().write(String.format("You are now connected to database \"%s\" as user \"%s\".%n",
          client.getCurrentDatabase(), client.getCurrentUser()));
    } else if (cmd.length == 2) {
      String databaseName = cmd[1];
      databaseName = databaseName.replace("\"", "");
      if (!client.existDatabase(databaseName)) {
        context.getOutput().write("Database '" + databaseName + "'  not found\n");
      } else {
        try {
          if (client.selectDatabase(databaseName)) {
            context.setCurrentDatabase(client.getCurrentDatabase());
            context.getOutput().write(String.format("You are now connected to database \"%s\" as user \"%s\".\n",
                context.getCurrentDatabase(), client.getCurrentUser()));
          } else {
            context.getOutput().write(String.format("cannot connect the database \"%s\".\n", databaseName));
          }
        } catch (Exception se) {
          if (se.getMessage() != null) {
            context.getOutput().write(se.getMessage() + "\n");
          } else {
            context.getOutput().write(String.format("cannot connect the database \"%s\".\n", databaseName));
          }
        }
      }
    }
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "connect to new database";
  }
}
