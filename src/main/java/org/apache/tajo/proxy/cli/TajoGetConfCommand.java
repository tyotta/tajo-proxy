package org.apache.tajo.proxy.cli;

import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

public class TajoGetConfCommand extends TajoProxyShellCommand {
  private TajoGetConf getconf;

  public TajoGetConfCommand(TajoProxyCliContext context) {
    super(context);
    getconf = new TajoGetConf(context.getConf(), context.getOutput(), context.getTajoProxyClient());
  }

  @Override
  public String getCommand() {
    return "\\getconf";
  }

  @Override
  public void invoke(String[] command) throws Exception {
    try {
      String[] getConfCommands = new String[command.length - 1];
      System.arraycopy(command, 1, getConfCommands, 0, getConfCommands.length);

      getconf.runCommand(getConfCommands);
    } catch (Exception e) {
      context.getOutput().println("ERROR: " + e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "<command> [options]";
  }

  @Override
  public String getDescription() {
    return "execute a tajo getconf command.";
  }
}
