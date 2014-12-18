package org.apache.tajo.proxy.cli;

import com.google.common.collect.Lists;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

public class UnsetCommand extends TajoProxyShellCommand {

  public UnsetCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\unset";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 2) {
      client.unsetSessionVariables(Lists.newArrayList(cmd[1]));
    } else {
      context.getOutput().println("usage: \\unset NAME");
    }
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "unset a session variable";
  }
}
