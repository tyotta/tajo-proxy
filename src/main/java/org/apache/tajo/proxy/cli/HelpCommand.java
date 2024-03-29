package org.apache.tajo.proxy.cli;

import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.cli.tsql.commands.TajoShellCommand;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;
import org.apache.tajo.util.VersionInfo;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;

public class HelpCommand extends TajoProxyShellCommand {
  private String targetDocVersion = "";

  public HelpCommand(TajoProxyCliContext context) {
    super(context);
  }

  @Override
  public String getCommand() {
    return "\\?";
  }

  @Override
  public String [] getAliases() {
    return new String [] {"\\help"};
  }

  @Override
  public void invoke(String[] cmd) throws Exception {
    if(targetDocVersion.equalsIgnoreCase("")) {
      targetDocVersion = getDocumentationVersion();
    }

    if (cmd.length == 1) {
      PrintWriter sout = context.getOutput();
      sout.println();

      sout.println("General");
      sout.println("  \\copyright    show Apache License 2.0");
      sout.println("  \\version      show Tajo version");
      sout.println("  \\?            show help");
      sout.println("  \\? [COMMAND]  show help of a given command");
      sout.println("  \\help         alias of \\?");
      sout.println("  \\q            quit tsql");
      sout.println();
      sout.println();

      sout.println("Informational");
      sout.println("  \\l           list databases");
      sout.println("  \\c           show current database");
      sout.println("  \\c [DBNAME]  connect to new database");
      sout.println("  \\d           list tables");
      sout.println("  \\d [TBNAME]  describe table");
      sout.println("  \\df          list functions");
      sout.println("  \\df NAME     describe function");
      sout.println("  \\lq          list queries");
      sout.println();
      sout.println();

      sout.println("Tool");
      sout.println("  \\!           execute a linux shell command");
      //sout.println("  \\dfs         execute a dfs command");
      sout.println();
      sout.println();

      sout.println("Variables");
      sout.println("  \\set [[NAME] [VALUE]  set session variable or list session variables");
      sout.println("  \\unset NAME           unset session variable");
      sout.println();
      sout.println();

      sout.println("Documentations");
      sout.println("  tsql guide        http://tajo.apache.org/docs/" + targetDocVersion + "/cli.html");
      sout.println("  Query language    http://tajo.apache.org/docs/" + targetDocVersion + "/sql_language.html");
      sout.println("  Functions         http://tajo.apache.org/docs/" + targetDocVersion + "/functions.html");
      sout.println("  Backup & restore  http://tajo.apache.org/docs/" + targetDocVersion + "/backup_and_restore.html");
      sout.println("  Configuration     http://tajo.apache.org/docs/" + targetDocVersion + "/configuration.html");
      sout.println();
    } else if (cmd.length == 2) {
      String slashCommand = "\\" + cmd[1];
      if (context.getCommands().containsKey(slashCommand)) {
        context.getCommands().get(slashCommand).printHelp();
      } else {
        context.getOutput().println("Command not found: " + cmd[1]);
      }
    }
  }

  private String getDocumentationVersion() {
    String tajoVersion = "", docVersion = "", docDefaultVersion = "current";
    String tajoFullVersion = VersionInfo.getVersion();

    int delimiterIdx = tajoFullVersion.indexOf("-");
    if (delimiterIdx > -1) {
      tajoVersion =  tajoFullVersion.substring(0, delimiterIdx);
    } else {
      tajoVersion = tajoFullVersion;
    }

    if(tajoVersion.equalsIgnoreCase("")) {
      docVersion = docDefaultVersion;
    } else {
      try {
        URL u = new URL("http://tajo.apache.org/docs/"+ tajoVersion + "/");
        HttpURLConnection huc =  (HttpURLConnection) u.openConnection();
        huc.setConnectTimeout(1000);
        huc.setReadTimeout(1000);
        huc.setRequestMethod("HEAD");
        if(huc.getResponseCode() == HttpURLConnection.HTTP_OK) {
          docVersion = tajoVersion;
        } else {
          docVersion = docDefaultVersion;
        }
      } catch (MalformedURLException e0) {
        docVersion = docDefaultVersion;
      } catch (ProtocolException e1) {
        docVersion = docDefaultVersion;
      } catch (IOException e2) {
        docVersion = docDefaultVersion;
      }
    }

    return docVersion;
  }

  @Override
  public String getUsage() {
    return "";
  }

  @Override
  public String getDescription() {
    return "show command lists and their usages";
  }
}
