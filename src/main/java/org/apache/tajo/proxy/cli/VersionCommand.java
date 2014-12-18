package org.apache.tajo.proxy.cli;

import org.apache.tajo.TajoConstants;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;
import org.apache.tajo.util.VersionInfo;

public class VersionCommand extends TajoProxyShellCommand {

  public VersionCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\version";
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    context.getOutput().println(VersionInfo.getRevision());
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "Tajo proxy shell";
  }
}
