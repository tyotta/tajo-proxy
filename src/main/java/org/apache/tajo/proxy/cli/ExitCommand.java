package org.apache.tajo.proxy.cli;

import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

public class ExitCommand extends TajoProxyShellCommand {

  public ExitCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\q";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    context.getOutput().println("bye!");
    System.exit(0);
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "quit";
  }
}
