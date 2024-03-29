package org.apache.tajo.proxy.cli;

import com.google.protobuf.ServiceException;
import org.apache.tajo.SessionVars;
import org.apache.tajo.util.StringUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.tajo.SessionVars.VariableMode;

public class SetCommandProxy extends TajoProxyShellCommand {

  public SetCommandProxy(TajoProxyCli.TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\set";
  }

  private void showAllSessionVars() throws IOException {
    for (Map.Entry<String, String> entry: client.getAllSessionVariables().entrySet()) {
      context.getOutput().println(StringUtils.quote(entry.getKey()) + "=" + StringUtils.quote(entry.getValue()));
    }
  }

  private void updateSessionVariable(String key, String val) throws IOException {
    Map<String, String> variables = new HashMap<String, String>();
    variables.put(key, val);
    client.updateSessionVariables(variables);
  }

  void set(String key, String val) throws IOException {
    SessionVars sessionVar = null;

    if (SessionVars.exists(key)) { // if the variable is one of the session variables
      sessionVar = SessionVars.get(key);

      // is it cli-side variable?
      if (sessionVar.getMode() == VariableMode.CLI_SIDE_VAR) {
        context.setCliSideVar(key, val);
      } else {
        updateSessionVariable(key, val);
      }

      if (SessionVars.isDeprecated(key)) {
        context.getOutput().println("Warning: deprecated to directly use config key in TajoConf.ConfVars. " +
            "Please execute '\\help set'.");
      }
    } else {
      updateSessionVariable(key, val);
    }
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if (cmd.length == 1) {
      showAllSessionVars();
    } else if (cmd.length == 3) {
      set(cmd[1], cmd[2]);
    } else {
      context.getOutput().println("usage: \\set [[NAME] VALUE]");
    }
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "set session variable or shows all session variables";
  }

  @Override
  public void printHelp() {
    context.getOutput().println("\nAvailable Session Variables:\n");
    for (SessionVars var : SessionVars.values()) {

      if (var.getMode() == VariableMode.DEFAULT ||
          var.getMode() == VariableMode.CLI_SIDE_VAR ||
          var.getMode() == VariableMode.FROM_SHELL_ENV) {

        context.getOutput().println("\\set " + var.keyname() + " " + getDisplayType(var.getVarType()) + " - " + var
            .getDescription());
      }
    }
  }

  public static String getDisplayType(Class<?> clazz) {
    if (clazz == String.class) {
      return "[text value]";
    } else if (clazz == Integer.class) {
      return "[int value]";
    } else if (clazz == Long.class) {
      return "[long value]";
    } else if (clazz == Float.class) {
      return "[real value]";
    } else if (clazz == Boolean.class) {
      return "[true or false]";
    } else {
      return clazz.getSimpleName();
    }
  }
}
