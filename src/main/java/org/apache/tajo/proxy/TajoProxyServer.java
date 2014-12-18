package org.apache.tajo.proxy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.webapp.QueryExecutorServlet;
import org.apache.tajo.webapp.StaticHttpServer;

import java.io.PrintWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;


public class TajoProxyServer extends CompositeService {
  private static final String VERSION = "0.9.0";
  private static final Log LOG = LogFactory.getLog(TajoProxyServer.class);
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;
  public static final String PROXY_RPC_ADDRESS = "tajo-proxy.proxy.rpc.address";
  public static final String PROXY_SERVER_WORKER_THREAD_NUM = "tajo-proxy.proxy.server.worker-thread-num";

  private TajoConf tajoConf;
  private ProxyClientRpcService clientRpcService;
  private int port;
  private StaticHttpServer webServer;
  private ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
  private JSPContext jspContext;

  public TajoProxyServer(int port) {
    super(TajoProxyServer.class.getName());
    this.port = port;
  }
  
  @Override
  public void init(Configuration conf) {
    this.tajoConf = (TajoConf)conf;

    clientRpcService = new ProxyClientRpcService(this, port);
    addService(clientRpcService);

    super.init(tajoConf);
  }

  private void initWebServer() throws Exception {
    InetSocketAddress address = tajoConf.getSocketAddr("tajo.proxy.info-http.address", "0.0.0.0", 38080);
      webServer = StaticHttpServer.getInstance(this ,"proxy",
          address.getHostName(), address.getPort(), true, null, tajoConf, null);
    webServer.start();
  }

  public JSPContext getJSPContext() {
    return jspContext;
  }

  @Override
  public void start() {
    LOG.info("TajoProxyServer startup");
    super.start();

    try {
      initWebServer();
      this.jspContext = new JSPContext(this);
      this.jspContext.init();
    } catch (Exception e) {
      LOG.error("Info WebServer init error: " + e.getMessage(), e);
    }
  }
  
  @Override
  public void stop() {
    super.stop();
    LOG.info("TajoProxyServer stopped");
  }

  public String getVersion() {
    return "tajo-proxy-server-" + VERSION;
  }

  public ProxyClientRpcService getClientRpcService() {
    return clientRpcService;
  }

  public String getProxyServerName() {
    return clientRpcService.getBindAddress().getHostName() + ":" + clientRpcService.getBindAddress().getPort();
  }

  String getThreadTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  public void dumpThread(Writer writer) {
    PrintWriter stream = new PrintWriter(writer);
    int STACK_DEPTH = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: Tajo Proxy Server");
    stream.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, STACK_DEPTH);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " + getThreadTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state + ", Blocked count: " + info.getBlockedCount() +
          ", Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime() + ", Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName() +
            ", Blocked by " + getThreadTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
      stream.println("");
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.out.println("Usage: java org.apache.tajo-proxy.proxy.ProxyServer <port>");
      System.exit(-1);
    }
    StringUtils.startupShutdownMessage(TajoProxyServer.class, args, LOG);
    
    TajoProxyServer proxyServer = new TajoProxyServer(Integer.parseInt(args[0]));
    ShutdownHookManager.get().addShutdownHook(new CompositeServiceShutdownHook(proxyServer), SHUTDOWN_HOOK_PRIORITY);

    TajoConf conf = new TajoConf();
    proxyServer.init(conf);
    proxyServer.start();
  }
}
