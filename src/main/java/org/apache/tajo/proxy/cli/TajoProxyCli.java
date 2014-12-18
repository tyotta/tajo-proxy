package org.apache.tajo.proxy.cli;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.ServiceException;
import jline.console.ConsoleReader;
import org.apache.commons.cli.*;
import org.apache.tajo.*;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.ipc.ClientProtos.GetQueryStatusResponse;
import org.apache.tajo.jdbc.proxy.TajoResultSet;
import org.apache.tajo.proxy.cli.SimpleParser.ParsingState;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.proxy.TajoProxyClient;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.TUtil;

import java.io.*;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.tajo.proxy.cli.ParsedResult.StatementType.META;
import static org.apache.tajo.proxy.cli.ParsedResult.StatementType.STATEMENT;

public class TajoProxyCli {
  public static final String ERROR_PREFIX = "ERROR: ";
  public static final String KILL_PREFIX = "KILL: ";

  private TajoConf conf;
  private TajoProxyClient client;
  private TajoProxyCliContext context;

  // Jline and Console related things
  private ConsoleReader reader;
  private InputStream sin;
  private PrintWriter sout;
  private TajoFileHistory history;

  // Current States
  private String currentDatabase;

  private TajoProxyCliOutputFormatter displayFormatter;

  private boolean wasError = false;

  private static final Class [] registeredCommands = {
      DescTableCommand.class,
      DescFunctionCommand.class,
      HelpCommand.class,
      ExitCommand.class,
      VersionCommand.class,
      ConnectDatabaseCommand.class,
      ListDatabaseCommand.class,
      SetCommandProxy.class,
      UnsetCommand.class,
      ExecExternalShellCommand.class,
      TajoGetConfCommand.class,
      QueryListCommand.class
  };
  private final Map<String, TajoProxyShellCommand> commands = new TreeMap<String, TajoProxyShellCommand>();

  protected static final Options options;
  private static final String HOME_DIR = System.getProperty("user.home");
  private static final String HISTORY_FILE = ".tajo_proxy_history";

  static {
    options = new Options();
    options.addOption("c", "command", true, "execute only single command, then exit");
    options.addOption("f", "file", true, "execute commands from file, then exit");
    options.addOption("h", "host:port", true, "TajoProxy server host:port");
    options.addOption("u", "user", true, "User ID");
    options.addOption("p", "password", true, "Password");
    options.addOption("conf", "conf", true, "configuration value");
    options.addOption("param", "param", true, "parameter value in SQL file");
    options.addOption("help", "help", false, "help");
  }

  public class TajoProxyCliContext extends OverridableConf {
    public TajoProxyCliContext(TajoConf conf) {
      super(conf, ConfigKey.ConfigType.SESSION);
    }

    public TajoProxyClient getTajoProxyClient() {
      return client;
    }

    public void setCurrentDatabase(String databasae) {
      currentDatabase = databasae;
    }

    public String getCurrentDatabase() {
      return currentDatabase;
    }

    public PrintWriter getOutput() {
      return sout;
    }

    public TajoConf getConf() {
      return conf;
    }

    @VisibleForTesting
    public String getCliSideVar(String key) {
      if (SessionVars.exists(key)) {
        ConfigKey configKey = SessionVars.get(key);
        return get(configKey);
      } else {
        return get(key);
      }
    }

    public void setCliSideVar(String key, String value) {
      Preconditions.checkNotNull(key);
      Preconditions.checkNotNull(value);

      boolean shouldReloadFormatter = false;

      if (SessionVars.exists(key)) {
        SessionVars configKey = SessionVars.get(key);
        put(configKey, value);
        shouldReloadFormatter = configKey.getMode() == SessionVars.VariableMode.CLI_SIDE_VAR;
      } else {
        set(key, value);

        // It is hard to recognize it is a client side variable. So, we always reload formatter.
        shouldReloadFormatter = true;
      }

      if (shouldReloadFormatter) {
        try {
          initFormatter();
        } catch (Exception e) {
          System.err.println(ERROR_PREFIX + e.getMessage());
        }
      }
    }

    public Map<String, TajoProxyShellCommand> getCommands() {
      return commands;
    }
  }

  private String queryPrefix;

  public TajoProxyCli(TajoConf c, String [] args, InputStream in, OutputStream out) throws Exception {
    try {
      this.conf = new TajoConf(c);
      context = new TajoProxyCliContext(conf);
      this.sin = in;
      this.reader = new ConsoleReader(sin, out);
      this.reader.setExpandEvents(false);
      this.sout = new PrintWriter(reader.getOutput());
      this.queryPrefix = conf.get("tajo.proxy.cli.query.prefix");
      initFormatter();

      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(options, args);

      if (cmd.hasOption("help")) {
        printUsage();
      }

      String user = null;

      if (cmd.hasOption("u")) {
        user = cmd.getOptionValue("u");
      } else {
        System.err.println(ERROR_PREFIX + "No user name: run with '-u' option.");
        System.exit(0);
      }

      String password = null;
      if (cmd.hasOption("p")) {
        password = cmd.getOptionValue("p");
      } else {
        password = new String(reader.readLine("Password: ", new Character('*')));
      }

      reader.setEchoCharacter(null);

      String baseDatabase = null;
      if (cmd.getArgList().size() > 0) {
        baseDatabase = (String) cmd.getArgList().get(0);
      }
      if (baseDatabase == null) {
        baseDatabase = conf.get("tajo.proxy.default.database");
      }

      if (cmd.getOptionValues("conf") != null) {
        processConfVarCommand(cmd.getOptionValues("conf"));
      }

      List<String> proxyServers = null;
      if (cmd.hasOption("h")) {
        proxyServers = TUtil.newList(cmd.getOptionValue("h").split(","));
      } else {
        if (conf.get(TajoProxyClient.TAJO_PROXY_SERVERS) != null) {
          proxyServers = TUtil.newList(conf.get(TajoProxyClient.TAJO_PROXY_SERVERS).split(","));
        }
      }

      if (proxyServers == null || proxyServers.isEmpty()) {
        System.err.println(ERROR_PREFIX + "cannot find valid TajoProxy server address");
        throw new RuntimeException("cannot find valid TajoProxy server address");

      } else {
        client = new TajoProxyClient(conf, proxyServers, baseDatabase);
      }
      client.login(user, password);

      context.setCurrentDatabase(client.getCurrentDatabase());

      initHistory();
      initCommands();

      if (cmd.getOptionValues("conf") != null) {
        processSessionVarCommand(cmd.getOptionValues("conf"));
      }

      if (cmd.hasOption("c")) {
        displayFormatter.setScirptMode();
        int exitCode = executeScript(cmd.getOptionValue("c"));
        sout.flush();
        System.exit(exitCode);
      }
      if (cmd.hasOption("f")) {
        displayFormatter.setScirptMode();
        cmd.getOptionValues("");
        File sqlFile = new File(cmd.getOptionValue("f"));
        if (sqlFile.exists()) {
          String script = FileUtil.readTextFile(new File(cmd.getOptionValue("f")));
          script = replaceParam(script, cmd.getOptionValues("param"));
          int exitCode = executeScript(script);
          sout.flush();
          System.exit(exitCode);
        } else {
          System.err.println(ERROR_PREFIX + "No such a file \"" + cmd.getOptionValue("f") + "\"");
          System.exit(-1);
        }
      }

      addShutdownHook();
    } catch (Exception e) {
      System.out.println("ERROR: " + e.getMessage());
      if (client != null) {
        client.close();
      }
      System.exit(0);
    }
  }

  private void processConfVarCommand(String[] confCommands) throws ServiceException {
    for (String eachParam: confCommands) {
      String[] tokens = eachParam.split("=");
      if (tokens.length != 2) {
        continue;
      }

      if (!SessionVars.exists(tokens[0])) {
        conf.set(tokens[0], tokens[1]);
      }
    }
  }

  private void processSessionVarCommand(String[] confCommands) throws IOException {
    for (String eachParam: confCommands) {
      String[] tokens = eachParam.split("=");
      if (tokens.length != 2) {
        continue;
      }

      if (SessionVars.exists(tokens[0])) {
        ((SetCommandProxy)commands.get("\\set")).set(tokens[0], tokens[1]);
      }
    }
  }

  private void initFormatter() throws Exception {
    Class formatterClass = Class.forName(context.get("tajo.cli.output.formatter",
        DefaultTajoCliOutputFormatter.class.getCanonicalName()));
    if (displayFormatter == null || !displayFormatter.getClass().equals(formatterClass)) {
      displayFormatter = (TajoProxyCliOutputFormatter)formatterClass.newInstance();
    }
    displayFormatter.init(context);
  }

  public TajoProxyCliContext getContext() {
    return context;
  }

  protected static String replaceParam(String script, String[] params) {
    if (params == null || params.length == 0) {
      return script;
    }

    for (String eachParam: params) {
      String[] tokens = eachParam.split("=");
      if (tokens.length != 2) {
        continue;
      }
      script = script.replace("${" + tokens[0] + "}", tokens[1]);
    }

    return script;
  }

  private void initHistory() {
    try {
      String historyPath = HOME_DIR + File.separator + HISTORY_FILE;
      if ((new File(HOME_DIR)).exists()) {
        history = new TajoFileHistory(new File(historyPath));
        reader.setHistory(history);
      } else {
        System.err.println(ERROR_PREFIX + "home directory : '" + HOME_DIR +"' does not exist.");
      }
    } catch (Exception e) {
      System.err.println(ERROR_PREFIX + e.getMessage());
    }
  }

  private void initCommands() {
    for (Class clazz : registeredCommands) {
      TajoProxyShellCommand cmd = null;
      try {
        Constructor cons = clazz.getConstructor(new Class[] {TajoProxyCliContext.class});
        cmd = (TajoProxyShellCommand) cons.newInstance(context);
      } catch (Exception e) {
        System.err.println(e.getMessage());
        throw new RuntimeException(e.getMessage());
      }
      commands.put(cmd.getCommand(), cmd);
      for (String alias : cmd.getAliases()) {
        commands.put(alias, cmd);
      }
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          history.flush();
        } catch (IOException e) {
        }
        if (client != null) {
          client.close();
        }
      }
    }));
  }

  private String updatePrompt(ParsingState state) throws ServiceException {
    if (state == ParsingState.WITHIN_QUOTE) {
      return "'";
    } else if (state == ParsingState.TOK_START) {
      return context.getCurrentDatabase();
    } else {
      return "";
    }
  }

  public int runShell() throws Exception {
    String line;
    String currentPrompt = context.getCurrentDatabase();
    int exitCode = 0;

    sout.write("Try \\? for help.\n");

    SimpleParser parser = new SimpleParser();
    while((line = reader.readLine(currentPrompt + "> ")) != null) {
      if (line.equals("")) {
        continue;
      }
      wasError = false;

      List<ParsedResult> parsedResults = parser.parseLines(line);

      if (parsedResults.size() > 0) {
        for (ParsedResult parsed : parsedResults) {
          history.addStatement(parsed.getHistoryStatement() + (parsed.getType() == STATEMENT ? ";" : ""));
        }
      }
      exitCode = executeParsedResults(parsedResults);
      currentPrompt = updatePrompt(parser.getState());

      if (exitCode != 0 && context.getBool(SessionVars.ON_ERROR_STOP)) {
        return exitCode;
      }
    }
    return exitCode;
  }

  private int executeParsedResults(Collection<ParsedResult> parsedResults) throws Exception {
    int exitCode = 0;
    for (ParsedResult parsedResult : parsedResults) {
      if (parsedResult.getType() == META) {
        exitCode = executeMetaCommand(parsedResult.getStatement());
      } else {
        exitCode = executeQuery(parsedResult.getStatement());
      }

      if (exitCode != 0) {
        return exitCode;
      }
    }

    return exitCode;
  }

  public int executeMetaCommand(String line) throws Exception {
    String [] metaCommands = line.split(";");
    for (String metaCommand : metaCommands) {
      String arguments [] = metaCommand.split(" ");

      TajoProxyShellCommand invoked = commands.get(arguments[0]);
      if (invoked == null) {
        printInvalidCommand(arguments[0]);
        wasError = true;
        return -1;
      }

      try {
        invoked.invoke(arguments);
      } catch (IllegalArgumentException ige) {
        displayFormatter.printErrorMessage(sout, ige);
        wasError = true;
        return -1;
      } catch (Exception e) {
        displayFormatter.printErrorMessage(sout, e);
        wasError = true;
        return -1;
      } finally {
        context.getOutput().flush();
      }

      if (wasError && context.getBool(SessionVars.ON_ERROR_STOP)) {
        break;
      }
    }

    return 0;
  }

  final static String DATE_FORMAT  = "yyyyMMddHHmmss";

  private String getQueryPrefix() {
    if (queryPrefix == null || queryPrefix.isEmpty()) {
      return "";
    }

    String evaluatedQueryPrefix = queryPrefix;
    if (!evaluatedQueryPrefix.startsWith("--")) {
      evaluatedQueryPrefix = "--" + evaluatedQueryPrefix;
    }
    SimpleDateFormat df = new SimpleDateFormat(DATE_FORMAT);
    evaluatedQueryPrefix = evaluatedQueryPrefix.replaceAll("[$]\\{current_timestamp\\}", df.format(new Date()));
    evaluatedQueryPrefix = evaluatedQueryPrefix.replaceAll("[$]\\{current_user\\}", client.getCurrentUser());

    evaluatedQueryPrefix += "\n";

    return evaluatedQueryPrefix;
  }

  private int executeQuery(String statement) throws IOException {
    long startTime = System.currentTimeMillis();

    String taggedSql = getQueryPrefix() + statement;

    GetQueryStatusResponse response = client.executeQuery(taggedSql);

    if (response == null) {
      displayFormatter.printErrorMessage(sout, "response is null");
      wasError = true;
    } else if (response.getResultCode() == ClientProtos.ResultCode.OK) {
      QueryId queryId = new QueryId(response.getQueryId());
      if (queryId.equals(QueryIdFactory.NULL_QUERY_ID) && !response.hasHasResult()) {
        displayFormatter.printMessage(sout, "OK");
      } else {
        waitForQueryCompleted(queryId);
      }
    } else {
      if (response.hasErrorMessage()) {
        displayFormatter.printErrorMessage(sout, response.getErrorMessage());
        wasError = true;
      } else {
        displayFormatter.printErrorMessage(sout, "Query is failed but no error message.");
        wasError = true;
      }
    }

    return wasError ? -1 : 0;
  }

  private void waitForQueryCompleted(QueryId queryId) {
    // query execute
    ResultSet res = null;
    QueryStatus status = null;
    try {

      int initRetries = 0;
      int progressRetries = 0;
      while (true) {
        // TODO - configurable
        status = client.getQueryStatus(queryId);

        if (status.getState() == QueryState.QUERY_RUNNING || status.getState() == QueryState.QUERY_SUCCEEDED) {
          displayFormatter.printProgress(sout, status);
        }

        if (!TajoProxyClient.isQueryRunnning(status.getState())) {
          break;
        } else {
          Thread.sleep(Math.min(200 * progressRetries, 1000));
          progressRetries += 2;
        }
      }

      if (status.getState() == QueryState.QUERY_ERROR || status.getState() == QueryState.QUERY_FAILED) {
        displayFormatter.printErrorMessage(sout, status);
        wasError = true;
      } else if (status.getState() == QueryState.QUERY_KILLED) {
        displayFormatter.printKilledMessage(sout, queryId);
        wasError = true;
      } else {
        if (status.getState() == QueryState.QUERY_SUCCEEDED) {
          float responseTime = ((float)(status.getFinishTime() - status.getSubmitTime()) / 1000.0f);

          res = client.getQueryResult(queryId);
          TableDesc desc = null;
          if (res instanceof TajoResultSet) {
            desc = new TableDesc(((TajoResultSet) res).getProxyQueryResult().getTableDesc());
          }
          displayFormatter.printResult(sout, sin, desc, responseTime, res);
        }
      }
    } catch (Throwable t) {
      displayFormatter.printErrorMessage(sout, t);
      wasError = true;
    } finally {
      if (res != null) {
        try {
          res.close();
        } catch (SQLException e) {
        }
      } else {
        if (status != null && status.getQueryId() != null) {
          client.closeQuery(status.getQueryId());
        }
      }
    }
  }

  public int executeScript(String script) throws Exception {
    wasError = false;
    List<ParsedResult> results = SimpleParser.parseScript(script);
    return executeParsedResults(results);
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("tsql [options] [database]", options);
  }

  private void printInvalidCommand(String command) {
    sout.println("Invalid command " + command + ". Try \\? for help.");
  }

  public void close() {
    //for testcase
    if (client != null) {
      client.close();
    }
  }

  public static void main(String [] args) throws Exception {
    TajoConf conf = new TajoConf();
    TajoProxyCli shell = new TajoProxyCli(conf, args, System.in, System.out);
    System.out.println();
    System.exit(shell.runShell());
  }
}
