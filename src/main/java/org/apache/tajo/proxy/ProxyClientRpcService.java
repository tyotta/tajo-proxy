package org.apache.tajo.proxy;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.*;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.client.TajoClient;
import org.apache.tajo.client.TajoClientImpl;
import org.apache.tajo.client.TajoClientUtil;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.ipc.ClientProtos.GetQueryStatusResponse.Builder;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.jdbc.TajoResultSetBase;
import org.apache.tajo.jdbc.FetchResultSet;
import org.apache.tajo.jdbc.proxy.TajoProxyClient;
import org.apache.tajo.proxy.TableAccessControlManager.AccessControlContext;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.*;
import org.apache.tajo.rpc.BlockingRpcServer;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.BoolProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueProto;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos.KeyValueSetProto;
import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.NetUtils;
import org.apache.tajo.util.TajoIdUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProxyClientRpcService extends AbstractService {
  private final static Log LOG = LogFactory.getLog(ProxyClientRpcService.class);

  private final BoolProto BOOL_TRUE =
      BoolProto.newBuilder().setValue(true).build();
  private final BoolProto BOOL_FALSE =
      BoolProto.newBuilder().setValue(false).build();

  private BlockingRpcServer rpcServer;
  private InetSocketAddress bindAddress;
  private int port;

  private TajoConf tajoConf;
  private ProxyServerClientProtocolHandler protocolHandler;

  private Map<TajoIdProtos.SessionIdProto, TajoClientHolder> tajoClientMap =
      new ConcurrentHashMap<TajoIdProtos.SessionIdProto, TajoClientHolder>();

  private Map<String, ResultSetHolder> queryResultSets = new HashMap<String, ResultSetHolder>();
  private Map<String, QuerySubmitTask> querySubmitTasks = new HashMap<String, QuerySubmitTask>();
  private ExecutorService executorService;
  private ResultSetAndTaskCleaner resultSetAndTaskCleaner;

  private int maxSession;

  private ProxyUserManageService userManager;
  private TableAccessControlManager accessContorlManager;

  //private List<QueryProgressInfo> finishedQueries = new ArrayList<QueryProgressInfo>();

  private TajoProxyServer proxyServer;

  public ProxyClientRpcService(TajoProxyServer proxyServer, int port) {
    super(ProxyClientRpcService.class.getName());
    this.port = port;
    this.proxyServer = proxyServer;
  }

  @Override
  public void init(Configuration conf) {
    this.tajoConf = (TajoConf)conf;
    try {
      int maxTaskRunner = tajoConf.getInt("tajo-proxy.proxyserver.max.taskrunner", 200);
      maxSession = tajoConf.getInt("tajo-proxy.proxyserver.max.session", 1000);

      this.executorService = Executors.newFixedThreadPool(maxTaskRunner);
      this.resultSetAndTaskCleaner = new ResultSetAndTaskCleaner();

    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e.getMessage(), e);
    }

    super.init(tajoConf);
  }

  @Override
  public void stop() {
    if (rpcServer != null) {
      rpcServer.shutdown();
    }

    if (executorService != null) {
      executorService.shutdownNow();
    }

    if (resultSetAndTaskCleaner != null) {
      resultSetAndTaskCleaner.interrupt();
    }

    synchronized (tajoClientMap) {
      for (TajoClientHolder eachClient : tajoClientMap.values()) {
        eachClient.tajoClient.close();
      }
    }

    if (userManager != null) {
      userManager.close();
    }
    super.stop();
  }

  public Collection<TajoClientHolder> getTajoClientSessions() {
    synchronized (tajoClientMap) {
      return Collections.unmodifiableCollection(tajoClientMap.values());
    }
  }

  @Override
  public void start() {
    try {
      try {
        userManager = new ProxyUserManageService();
        userManager.init(tajoConf);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        LOG.fatal("TajoProxyServer shutdown because can't load tajo-user.xml");
        System.exit(0);
      }

      accessContorlManager = new TableAccessControlManager(userManager);

      String rpcAddress = tajoConf.get(TajoProxyServer.PROXY_RPC_ADDRESS);
      if (rpcAddress == null) {
        rpcAddress = InetAddress.getLocalHost().getHostName();
      }
      InetSocketAddress initIsa = NetUtils.createSocketAddr(rpcAddress, port);
      int workerNum = tajoConf.getInt(TajoProxyServer.PROXY_SERVER_WORKER_THREAD_NUM,
          Runtime.getRuntime().availableProcessors() * 1);

      protocolHandler = new ProxyServerClientProtocolHandler();
      rpcServer = new BlockingRpcServer(ProxyServerClientProtocol.class, protocolHandler, initIsa, workerNum);
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
    rpcServer.start();

    bindAddress = NetUtils.getConnectAddress(rpcServer.getListenAddress());
    port = bindAddress.getPort();

    LOG.info("Instantiated ProxyClientService at " + this.bindAddress);
    super.start();
  }

  public ResultSet createNullResultSet(TajoClient tajoClient, QueryId queryId) throws IOException {
    return new TajoResultSet(tajoClient, queryId);
  }

  public ProxyServerClientProtocolHandler getProtocolHandler() {
    return protocolHandler;
  }

  private static GetQueryStatusResponse createErrorResponse(QueryId queryId , String errorMessagge) {
    Builder responseBuilder = GetQueryStatusResponse.newBuilder();
    responseBuilder.setQueryId(queryId.getProto());
    responseBuilder.setResultCode(ResultCode.ERROR);
    responseBuilder.setState(TajoProtos.QueryState.QUERY_ERROR);
    responseBuilder.setErrorMessage(errorMessagge);

    return responseBuilder.build();
  }

  public ResultSetHolder getResultSetHolder(String key) {
    synchronized (queryResultSets) {
      return queryResultSets.get(key);
    }
  }

  public QueryProgressInfo getQuerySubmitTask(String key) {
    synchronized (querySubmitTasks) {
      if (querySubmitTasks.containsKey(key)) {
        return querySubmitTasks.get(key).queryProgressInfo;
      } else {
        return null;
      }
    }
  }

  public Collection<QueryProgressInfo> getQuerySubmitTasks() {
    synchronized (querySubmitTasks) {
      List<QueryProgressInfo> infos = new ArrayList<QueryProgressInfo>();
      for (QuerySubmitTask eachTask: querySubmitTasks.values()) {
        infos.add(eachTask.queryProgressInfo);
      }
      return infos;
    }
  }

  public ProxyUserManageService getUserManager() {
    return userManager;
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }

  public TajoClient getTajoClient(TajoIdProtos.SessionIdProto sessionId) throws ServiceException {
    if (sessionId == null || !sessionId.hasId()) {
      throw new ServiceException("No sessionId");
    }

    synchronized (tajoClientMap) {
      if (tajoClientMap.size() >= maxSession) {
        throw new ServiceException("exceed max session [" + maxSession + "]");
      }
      TajoClientHolder tajoClientHolder = tajoClientMap.get(sessionId);

      //if there is multiple proxy server, TajoProxyClient call randomly. So certain proxy server hasn't session.
      if (tajoClientHolder == null) {
        //throw new ServiceException("No session info:" + sessionId.getId());
        try {
          TajoClient tajoClient = new TajoClientImpl(tajoConf);
          tajoClient.setSessionId(sessionId);
          tajoClientHolder = new TajoClientHolder();
          tajoClientHolder.tajoClient = tajoClient;
          tajoClientHolder.lastTouchTime = System.currentTimeMillis();

          tajoClientMap.put(sessionId, tajoClientHolder);
          return tajoClient;
        } catch (Exception e) {
          throw new ServiceException(e.getMessage(), e);
        }
      } else {
        tajoClientHolder.lastTouchTime = System.currentTimeMillis();
        return tajoClientHolder.tajoClient;
      }
    }
  }

  public void touchTajoClient(TajoIdProtos.SessionIdProto sessionId)  {
    if (sessionId == null || !sessionId.hasId()) {
      return;
    }

    synchronized (tajoClientMap) {
      TajoClientHolder tajoClientHolder = tajoClientMap.get(sessionId);

      if (tajoClientHolder == null) {
        return;
      } else {
        tajoClientHolder.lastTouchTime = System.currentTimeMillis();
      }
    }
  }

  public static class TajoClientHolder {
    TajoClient tajoClient;
    long lastTouchTime;

    public TajoClient getTajoClient() {
      return tajoClient;
    }

    public long getLastTouchTime() {
      return lastTouchTime;
    }
  }

  public static class ResultSetHolder {
    TajoIdProtos.SessionIdProto sessionId;
    QueryId queryId;
    TajoResultSetBase rs;
    TableDesc tableDesc;
    Schema schema;
    long lastTouchTime;

    private String getKey() {
      return getKey(sessionId, queryId);
    }

    public static String getKey(TajoIdProtos.SessionIdProto sessionId, QueryId queryId) {
      return sessionId.getId() + "," + queryId.toString();
    }
  }

  public static class QueryProgressInfo {
    String userId;
    QueryId queryId;
    String query;
    GetQueryStatusResponse queryStatus;
    GetQueryStatusRequest statusRequest;
    QueryExternalParam externalParam;
    SessionIdProto sessionId;
    long lastTouchTime;

    public QueryProgressInfo() {
    }

    public QueryProgressInfo(SessionIdProto sessionId) {
      this.sessionId = sessionId;
    }

    public String getRealUserId() {
      if (externalParam == null) {
        return userId;
      } else {
        return externalParam.userId;
      }
    }

    public String getRealQuery() {
      if (externalParam != null) {
        return externalParam.getRealQuery();
      } else {
        return query;
      }
    }

    public QueryProgressInfo clone() {
      QueryProgressInfo cloneInfo = new QueryProgressInfo();

      cloneInfo.userId = userId;
      cloneInfo.queryId = new QueryId(queryId.getProto());
      cloneInfo.query = query;
      cloneInfo.lastTouchTime = lastTouchTime;
      cloneInfo.sessionId = SessionIdProto.newBuilder().mergeFrom(sessionId).build();

      if (queryStatus != null) {
        cloneInfo.queryStatus = GetQueryStatusResponse.newBuilder().mergeFrom(queryStatus).build();
      }
      if (statusRequest != null) {
        cloneInfo.statusRequest = GetQueryStatusRequest.newBuilder().mergeFrom(statusRequest).build();
      }
      if (externalParam != null) {
        cloneInfo.externalParam = externalParam.clone();
      }
      return cloneInfo;
    }

    public String getUserId() {
      return userId;
    }

    public QueryId getQueryId() {
      return queryId;
    }

    public String getQuery() {
      return query;
    }

    public GetQueryStatusResponse getQueryStatus() {
      return queryStatus;
    }

    public GetQueryStatusRequest getStatusRequest() {
      return statusRequest;
    }

    public QueryExternalParam getExternalParam() {
      return externalParam;
    }

    public SessionIdProto getSessionId() {
      return sessionId;
    }

    public long getLastTouchTime() {
      return lastTouchTime;
    }
  }

  //--yyyymmddhhmiss_userid_queryType_arg1_arg2
  static Pattern externalParamPattern = Pattern.compile("^(-{2})(\\d{14})_(\\w*)_((QRY|SCH){1})");

  public static QueryExternalParam paresQueryExternalParam(String query) {
    query = query.trim();
    Matcher m = externalParamPattern.matcher(query);
    if (!m.find()) {
      return null;
    }
    if (m.start() != 0) {
      return null;
    }
    String[] tokens = query.substring(m.start(), m.end()).split("_");
    QueryExternalParam externalParam = new QueryExternalParam();

    externalParam.submitTime = tokens[0].substring(2);
    externalParam.userId = tokens[1];
    externalParam.queryType = tokens[2];
    int index = query.indexOf("\n", m.end());
    if (index > 0) {
      String[] args = query.substring(m.end() + 1, index).split("_");
      if (args.length == 2) {
        externalParam.arg1 = args[0];
        externalParam.arg2 = args[1];
      }
      externalParam.realQuery = query.substring(index + 1);
    } else {
      externalParam.realQuery = query;
    }
    return externalParam;
  }

  class ProxyServerClientProtocolHandler implements ProxyServerClientProtocolService.BlockingInterface {
    private GetQueryStatusResponse makeErrorResponse(GetQueryStatusResponse.Builder responseBuilder,
                                               String errorMessage) {
      LOG.error("Query error:" + errorMessage);
      responseBuilder.setErrorMessage(errorMessage);
      responseBuilder.setState(TajoProtos.QueryState.QUERY_ERROR);
      responseBuilder.setResultCode(ResultCode.ERROR);
      QueryId queryId = QueryIdFactory.newQueryId(0, 0);  //DUMMY
      responseBuilder.setQueryId(queryId.getProto());

      return responseBuilder.build();
    }

    @Override
    public GetQueryStatusResponse submitQuery(RpcController controller,
                                              QueryRequest request) throws ServiceException {
      LOG.debug("Run Query:" + request.getQuery());
      GetQueryStatusResponse.Builder responseBuilder = GetQueryStatusResponse.newBuilder();
      try {
        ProxyUser user = userManager.getSessionUser(request.getSessionId().getId());
        if (user == null) {
          return makeErrorResponse(responseBuilder, "Not login user.");
        }
        TajoClient tajoClient = getTajoClient(request.getSessionId());

        AccessControlContext accessControlContext =
            accessContorlManager.checkTablePermission(tajoClient.getCurrentDatabase(), request);
        if (!accessControlContext.isPermission()) {
          return makeErrorResponse(responseBuilder, accessControlContext.getErrorMessage());
        }

        SubmitQueryResponse response = tajoClient.executeQuery(request.getQuery());

        if (response.hasErrorMessage()) {
          return makeErrorResponse(responseBuilder, response.getErrorMessage());
        }

        QueryExternalParam externalParam = paresQueryExternalParam(request.getQuery());
        if (response.getIsForwarded()) {
          QuerySubmitTask querySubmitTask = new QuerySubmitTask(request.getSessionId());
          querySubmitTask.queryProgressInfo.queryId = new QueryId(response.getQueryId());

          querySubmitTask.queryProgressInfo.userId = user.getUserId();
          QueryStatus queryStatus = tajoClient.getQueryStatus(querySubmitTask.queryProgressInfo.queryId);
          querySubmitTask.queryProgressInfo.lastTouchTime = System.currentTimeMillis();

          responseBuilder.setQueryId(response.getQueryId());
          responseBuilder.setResultCode(ResultCode.OK);
          responseBuilder.setState(queryStatus.getState());
          responseBuilder.setProgress(queryStatus.getProgress());
          responseBuilder.setSubmitTime(queryStatus.getSubmitTime());
          responseBuilder.setFinishTime(queryStatus.getFinishTime());
          responseBuilder.setHasResult(queryStatus.hasResult());

          if (queryStatus.getErrorMessage() != null) {
            responseBuilder.setErrorMessage(queryStatus.getErrorMessage());
          }

          if (queryStatus.getQueryMasterHost() != null) {
            responseBuilder.setQueryMasterHost(queryStatus.getQueryMasterHost());
            responseBuilder.setQueryMasterPort(queryStatus.getQueryMasterPort());
          }

          querySubmitTask.queryProgressInfo.queryStatus = responseBuilder.build();
          querySubmitTask.queryProgressInfo.externalParam = externalParam;
          querySubmitTask.queryProgressInfo.query = request.getQuery();

          synchronized (querySubmitTasks) {
            LOG.info(request.getSessionId().getId() + "," + querySubmitTask.queryProgressInfo.queryId + " query started");
            querySubmitTasks.put(querySubmitTask.getKey(), querySubmitTask);
          }
          executorService.submit(querySubmitTask);
          return querySubmitTask.queryProgressInfo.queryStatus;
        } else {
          //select * from table limit 100
          QueryId queryId = new QueryId(response.getQueryId());
          LOG.info(request.getSessionId().getId() + "," + queryId + " query is started(direct query)");

          QuerySubmitTask querySubmitTask = new QuerySubmitTask(request.getSessionId());
          querySubmitTask.queryProgressInfo.queryId = queryId;
          querySubmitTask.queryProgressInfo.userId = user.getUserId();
          querySubmitTask.queryProgressInfo.lastTouchTime = System.currentTimeMillis();
          querySubmitTask.queryProgressInfo.externalParam = externalParam;
          querySubmitTask.queryProgressInfo.query = request.getQuery();

          responseBuilder.setQueryId(response.getQueryId());
          responseBuilder.setResultCode(ResultCode.OK);
          responseBuilder.setState(TajoProtos.QueryState.QUERY_SUCCEEDED);
          responseBuilder.setProgress(1.0f);
          responseBuilder.setSubmitTime(System.currentTimeMillis());
          responseBuilder.setFinishTime(System.currentTimeMillis());
          responseBuilder.setHasResult(true);

          querySubmitTask.queryProgressInfo.queryStatus = responseBuilder.build();

          synchronized (querySubmitTasks) {
            querySubmitTasks.put(querySubmitTask.getKey(), querySubmitTask);
          }

          ResultSet resultSet = TajoClientUtil.createResultSet(tajoConf, tajoClient, response);
          synchronized (queryResultSets) {
            ResultSetHolder rsHolder = new ResultSetHolder();
            rsHolder.sessionId = request.getSessionId();
            rsHolder.queryId = queryId;
            rsHolder.rs = (TajoResultSetBase) resultSet;
            rsHolder.tableDesc = null;
            if (resultSet instanceof FetchResultSet) {
             rsHolder.tableDesc = new TableDesc(response.getTableDesc());
            } else if (resultSet instanceof TajoMemoryResultSet) {
              rsHolder.schema = new Schema(response.getResultSet().getSchema());
            }
            rsHolder.lastTouchTime = System.currentTimeMillis();
            queryResultSets.put(rsHolder.getKey(), rsHolder);
          }
          return querySubmitTask.queryProgressInfo.queryStatus;
        }
      } catch (Throwable e) {
        LOG.error(e.getMessage(), e);
        checkTajoInvalidSession(e, request.getSessionId());
        responseBuilder.setErrorMessage(StringUtils.stringifyException(e));
        responseBuilder.setState(TajoProtos.QueryState.QUERY_ERROR);
        responseBuilder.setResultCode(ResultCode.ERROR);
        QueryId queryId = QueryIdFactory.newQueryId(0, 0);  //DUMMY
        responseBuilder.setQueryId(queryId.getProto());
        return responseBuilder.build();
      }
    }

    @Override
    public ProxyQueryResult getQueryResult(RpcController controller,
                                           ProxyQueryResultRequest request) throws ServiceException {
      GetQueryResultRequest queryResultRequest = request.getResultRequestMeta();
      QueryId queryId = new QueryId(queryResultRequest.getQueryId());

      ProxyQueryResult.Builder resultBuilder = ProxyQueryResult.newBuilder();

      synchronized(querySubmitTasks) {
        QuerySubmitTask querySubmitTask =
            querySubmitTasks.get(ResultSetHolder.getKey(queryResultRequest.getSessionId(), queryId));
        if (querySubmitTask == null) {
          LOG.warn("No query submit info for " + queryResultRequest.getSessionId().getId() + "," + queryId);
          resultBuilder.setQueryStatus(createErrorResponse(queryId, "No query submit info for " + queryId));
          return resultBuilder.build();
        } else {
          resultBuilder.setQueryStatus(querySubmitTask.queryProgressInfo.queryStatus);
        }
      }

      ResultSetHolder rsHolder = null;
      synchronized (queryResultSets) {
        rsHolder = queryResultSets.get(ResultSetHolder.getKey(queryResultRequest.getSessionId(), queryId));
      }

      if (rsHolder == null) {
        LOG.warn("No QueryResult for:" + queryResultRequest.getSessionId().getId() + "," + queryId);
        resultBuilder.setQueryStatus(createErrorResponse(queryId, "No query result for " + queryId));
        return resultBuilder.build();
      } else {
        try {
          rsHolder.lastTouchTime = System.currentTimeMillis();

          RowStoreUtil.RowStoreEncoder rowEncoder = null;
          if (rsHolder.tableDesc != null)  {
            resultBuilder.setTableDesc(rsHolder.tableDesc.getProto());
            rowEncoder = RowStoreUtil.createEncoder(rsHolder.tableDesc.getSchema());
          } else {
            resultBuilder.setSchema(rsHolder.schema.getProto());
            rowEncoder = RowStoreUtil.createEncoder(rsHolder.schema);
          }

          int fetchSize = request.getFetchSize();
          if (fetchSize <= 0) {
            LOG.warn("Fetch size(" + fetchSize + ") is less than 0, use default size:" +
                TajoProxyClient.DEFAULT_FETCH_SIZE);
            fetchSize = TajoProxyClient.DEFAULT_FETCH_SIZE;
          }
          List<ByteString> rows = new ArrayList<ByteString>();
          int rowCount = 0;

          while (rsHolder.rs.next()) {
            Tuple tuple = rsHolder.rs.getCurrentTuple();
            rows.add(ByteString.copyFrom((rowEncoder.toBytes(tuple))));
            rowCount++;
            if (rowCount >= fetchSize) {
              break;
            }
          }
          LOG.info("Send result to client for " +
              queryResultRequest.getSessionId().getId() + "," + queryId + ", " + rowCount + " rows");
          resultBuilder.addAllRows(rows);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);

          resultBuilder.setQueryStatus(
              createErrorResponse(queryId, "Error while result fetching " + queryId + " cause:\n" +
                  StringUtils.stringifyException(e)));
        }
        return resultBuilder.build();
      }
    }

    @Override
    public GetQueryListResponse getQueryList(RpcController controller,
                                                    GetQueryListRequest request) throws ServiceException {
      List<ClientProtos.BriefQueryInfo> runningQueryList = getTajoClient(request.getSessionId()).getRunningQueryList();
      List<ClientProtos.BriefQueryInfo> finishedQueryList = getTajoClient(request.getSessionId()).getFinishedQueryList();
      return GetQueryListResponse.newBuilder()
          .addAllQueryList(runningQueryList)
          .addAllQueryList(finishedQueryList)
          .build();
    }

    @Override
    public GetQueryStatusResponse getQueryStatus(RpcController controller,
                                                 GetQueryStatusRequest request) throws ServiceException {
      touchTajoClient(request.getSessionId());
      QueryId queryId = new QueryId(request.getQueryId());
      synchronized(querySubmitTasks) {
        QuerySubmitTask querySubmitTask = querySubmitTasks.get(ResultSetHolder.getKey(request.getSessionId(), queryId));
        if (querySubmitTask == null) {
          return createErrorResponse(queryId, "No query submit info for " + queryId);
        }

        querySubmitTask.queryProgressInfo.lastTouchTime = System.currentTimeMillis();
        return querySubmitTask.queryProgressInfo.queryStatus;
      }
    }

    @Override
    public ServerResponse killQuery(RpcController controller, QueryIdRequest killQueryRequest)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (!userManager.isAdmin(killQueryRequest.getSessionId().getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          QueryId queryId = new QueryId(killQueryRequest.getQueryId());
          QueryStatus queryStatus = getTajoClient(killQueryRequest.getSessionId()).killQuery(queryId);
          boolean result = queryStatus.getState() == TajoProtos.QueryState.QUERY_KILLED ||
              queryStatus.getState() == TajoProtos.QueryState.QUERY_KILL_WAIT;

          builder.setBoolResult(result ? BOOL_TRUE : BOOL_FALSE);
          builder.setResultCode(ResultCode.OK);
          removeQueryTask(killQueryRequest.getSessionId(), queryId);
        }
      } catch (Exception e) {
        checkTajoInvalidSession(e, killQueryRequest.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        LOG.error(e.getMessage(), e);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public GetClusterInfoResponse getClusterInfo(RpcController controller,
                                                 GetClusterInfoRequest request) throws ServiceException {
      List<ClientProtos.WorkerResourceInfo> workers = getTajoClient(request.getSessionId()).getClusterInfo();
      return GetClusterInfoResponse.newBuilder().addAllWorkerList(workers).build();
    }

    @Override
    public ServerResponse existTable(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (userManager.isAdmin(request.getSessionId().getId())) {
          boolean existsResult = getTajoClient(request.getSessionId()).existTable(request.getValue());
          builder.setBoolResult(existsResult ? BOOL_TRUE : BOOL_FALSE);
          builder.setResultCode(ResultCode.OK);
        } else {
          boolean existsResult = getUserGrantedTables(request.getSessionId(),
              getTajoClient(request.getSessionId()).getCurrentDatabase()).contains(request.getValue());
          builder.setBoolResult(existsResult ? BOOL_TRUE : BOOL_FALSE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        checkTajoInvalidSession(e, request.getSessionId());
        LOG.error(e.getMessage(), e);
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    private List<String> getUserGrantedTables(SessionIdProto sessionId, String databaseName) throws ServiceException {
      List<String> tables = null;
      Set<String> allTables = userManager.getUserGrantedTables(sessionId.getId());
      if (allTables.size() == 1 && allTables.iterator().next().equals("*")) {
        tables = getTajoClient(sessionId).getTableList(databaseName);
      } else {
        tables = new ArrayList<String>();
        for (String eachTable : allTables) {
          String[] tokens = eachTable.split("\\.");
          if (tokens.length == 2) {
            String tableDatabaseName = tokens[0];
            String tableName = tokens[1];

            if (databaseName != null && databaseName.equals(tableDatabaseName)) {
              tables.add(tableName);
            }
          }
        }
      }

      return tables;
    }

    @Override
    public GetTableListResponse getTableList(RpcController controller,
                                             GetTableListRequest request) throws ServiceException {
      try {
        String databaseName = request.getDatabaseName();
        List<String> tables = null;
        if (userManager.isAdmin(request.getSessionId().getId())) {
          tables = getTajoClient(request.getSessionId()).getTableList(databaseName);
        } else {
          tables = getUserGrantedTables(request.getSessionId(), databaseName);
        }
        return GetTableListResponse.newBuilder()
            .addAllTables(tables)
            .build();
      } catch (Exception e) {
        LOG.error(e.getMessage());
        checkTajoInvalidSession(e, request.getSessionId());
        throw new ServiceException(e.getMessage(), e);
      }
    }

    @Override
    public TableResponse getTableDesc(RpcController controller,
                                      GetTableDescRequest request) throws ServiceException {
      try {
        String databaseName;
        String tableName;
        if (CatalogUtil.isFQTableName(request.getTableName())) {
          String [] splitted = CatalogUtil.splitFQTableName(request.getTableName());
          databaseName = splitted[0];
          tableName = splitted[1];
        } else {
          databaseName = getTajoClient(request.getSessionId()).getCurrentDatabase();
          tableName = request.getTableName();
        }

        TableDesc tableDesc = getTajoClient(request.getSessionId()).getTableDesc(request.getTableName());
        if (tableDesc == null) {
          return TableResponse.newBuilder()
              .setResultCode(ResultCode.ERROR)
              .setErrorMessage("No such table " + databaseName + "." + request.getTableName())
              .build();
        } else {
          List<String> tables = null;
          if (userManager.isAdmin(request.getSessionId().getId())) {
            tables = getTajoClient(request.getSessionId()).getTableList(databaseName);
          } else {
            tables = getUserGrantedTables(request.getSessionId(), databaseName);
          }

          if (tables != null && !tables.contains(tableName)) {
            return TableResponse.newBuilder()
                .setResultCode(ResultCode.ERROR)
                .setErrorMessage("No such table " + databaseName + "." + request.getTableName())
                .build();
          }

          return TableResponse.newBuilder()
              .setResultCode(ResultCode.OK)
              .setTableDesc(tableDesc.getProto())
              .build();
        }
      } catch (Exception e) {
        LOG.error(e.getMessage());
        checkTajoInvalidSession(e, request.getSessionId());
        return TableResponse.newBuilder()
            .setResultCode(ResultCode.ERROR)
            .setErrorMessage(StringUtils.stringifyException(e))
            .build();
      }
    }

    @Override
    public ServerResponse dropTable(RpcController controller, DropTableRequest request)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (!userManager.isAdmin(request.getSessionId().getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          boolean existsResult = getTajoClient(request.getSessionId()).dropTable(request.getName());
          builder.setBoolResult(existsResult ? BOOL_TRUE : BOOL_FALSE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse createDatabase(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (!userManager.isAdmin(request.getSessionId().getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          boolean result = getTajoClient(request.getSessionId()).createDatabase(request.getValue());
          builder.setBoolResult(result ? BOOL_TRUE : BOOL_FALSE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse dropDatabase(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (!userManager.isAdmin(request.getSessionId().getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          boolean result = getTajoClient(request.getSessionId()).dropDatabase(request.getValue());
          builder.setBoolResult(result ? BOOL_TRUE : BOOL_FALSE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse existDatabase(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        boolean result = getTajoClient(request.getSessionId()).existDatabase(request.getValue());
        builder.setBoolResult(result ? BOOL_TRUE : BOOL_FALSE);
        builder.setResultCode(ResultCode.OK);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public KeyValueSetProto getAllSessionVariables(RpcController controller, SessionIdProto request) throws ServiceException {
      TajoClient tajoClient = getTajoClient(request);

      List<KeyValueProto> keyValueProtoList = new ArrayList<KeyValueProto>();
      for (Map.Entry<String, String> eachValue: tajoClient.getAllSessionVariables().entrySet()) {
        keyValueProtoList.add(
            KeyValueProto.newBuilder()
                .setKey(eachValue.getKey())
                .setValue(eachValue.getValue())
                .build());
      }

      KeyValueSetProto.Builder builder = KeyValueSetProto.newBuilder();
      builder.addAllKeyval(keyValueProtoList);

      return builder.build();
    }

    @Override
    public FunctionResponse getFunctionList(RpcController controller,
                                            SessionedStringProto request) throws ServiceException {
      FunctionResponse.Builder builder = FunctionResponse.newBuilder();

      try {
        List<CatalogProtos.FunctionDescProto> functions =
            getTajoClient(request.getSessionId()).getFunctions(request.getValue());

        builder.addAllFunctions(functions);
        builder.setResultCode(ResultCode.OK);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        builder.setResultCode(ResultCode.ERROR);
        builder.setErrorMessage(StringUtils.stringifyException(e));
      }
      return builder.build();
    }

    @Override
    public ServerResponse closeQuery(RpcController controller,
                                     ProxyServerClientProtocol.CloseQueryRequest closeQueryRequest)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();

      try {
        QueryId queryId = new QueryId(closeQueryRequest.getQueryId());
        getTajoClient(closeQueryRequest.getSessionId()).closeQuery(queryId);
        removeQueryTask(closeQueryRequest.getSessionId(), queryId);
        builder.setResultCode(ResultCode.OK);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }
      return builder.build();
    }

    @Override
    public ServerResponse createSession(RpcController controller,
                                        CreateProxySessionRequest sessionRequest) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (tajoClientMap.size() >= maxSession) {
          String errorMesasage = "exceed max session [" + maxSession + "]";
          builder.setResultCode(ResultCode.ERROR);
          builder.setErrorMessage(errorMesasage);
        } else {
          if (!userManager.login(sessionRequest.getUser())) {
            builder.setResultCode(ResultCode.ERROR);
            builder.setBoolResult(BOOL_FALSE);
            builder.setErrorMessage(sessionRequest.getUser().getUserId() + " does not exists or wrong password.");
            return builder.build();
          }
          String dbName = sessionRequest.getDefaultDatabase();
          dbName = (dbName == null || dbName.isEmpty()) ? TajoConstants.DEFAULT_DATABASE_NAME : dbName;
          TajoClient tajoClient = new TajoClientImpl(tajoConf, dbName);
          tajoClient.getCurrentDatabase();
          TajoIdProtos.SessionIdProto sessionId = tajoClient.getSessionId();
          if (sessionId == null) {
            throw new ServiceException("Can't make tajo session.");
          }

          TajoClientHolder holder = new TajoClientHolder();
          holder.tajoClient = tajoClient;
          holder.lastTouchTime = System.currentTimeMillis();
          synchronized (tajoClientMap) {
            tajoClientMap.put(sessionId, holder);
          }
          userManager.addLoginUser(sessionId.getId(), sessionRequest.getUser());
          builder.setBoolResult(BOOL_TRUE);
          builder.setResultCode(ResultCode.OK);
          builder.setSessionId(sessionId);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse closeSession(RpcController controller,
                                       TajoIdProtos.SessionIdProto sessionId) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        synchronized (tajoClientMap) {
          TajoClientHolder clientHolder = tajoClientMap.remove(sessionId);
          if (clientHolder != null && clientHolder.tajoClient != null) {
            clientHolder.tajoClient.close();
          }
        }
        builder.setResultCode(ResultCode.OK);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }
      return builder.build();
    }

    @Override
    public BoolProto updateSessionVariables(RpcController controller, UpdateSessionVariableRequest request)
        throws ServiceException {
      TajoClient tajoClient = getTajoClient(request.getSessionId());

      if (request.getSessionVars() != null) {
        Map<String, String> sessionVariable = new HashMap<String, String>();
        for (KeyValueProto eachVariable : request.getSessionVars().getKeyvalList()) {
          sessionVariable.put(eachVariable.getKey(), eachVariable.getValue());
        }
        tajoClient.updateSessionVariables(sessionVariable);
      }

      if (request.getUnsetVariablesCount() > 0) {
        tajoClient.unsetSessionVariables(request.getUnsetVariablesList());
      }
      return BOOL_TRUE;
    }

    @Override
    public PrimitiveProtos.StringProto getCurrentDatabase(RpcController controller,
                                                          TajoIdProtos.SessionIdProto sessionId) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      String database = getTajoClient(sessionId).getCurrentDatabase();
      return PrimitiveProtos.StringProto.newBuilder().setValue(database).build();
    }

    @Override
    public ServerResponse selectDatabase(RpcController controller, SessionedStringProto request)
        throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        boolean result = getTajoClient(request.getSessionId()).selectDatabase(request.getValue()).booleanValue();
        builder.setBoolResult(result ? BOOL_TRUE : BOOL_FALSE);
        builder.setResultCode(ResultCode.OK);
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public PrimitiveProtos.StringListProto getAllDatabases(RpcController controller,
                                                           TajoIdProtos.SessionIdProto sessionId) throws ServiceException {
      List<String> databases = getTajoClient(sessionId).getAllDatabaseNames();

      return PrimitiveProtos.StringListProto.newBuilder().addAllValues(databases).build();
    }

    @Override
    public ServerResponse addGroup(RpcController controller, SessionedStringProto request) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        SessionIdProto sessionId = request.getSessionId();
        if (!userManager.isAdmin(sessionId.getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          boolean success = userManager.addGroup(request.getValue());
          if (success) {
            builder.setBoolResult(BOOL_TRUE);
            builder.setResultCode(ResultCode.OK);
          } else {
            builder.setResultCode(ResultCode.ERROR);
            builder.setBoolResult(BOOL_FALSE);
            builder.setErrorMessage(request.getValue() + " already exists.");
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse reloadProxyUserMeta(RpcController controller, SessionIdProto sessionId) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        if (!userManager.isAdmin(sessionId.getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          userManager.loadTajoUsers();
          builder.setBoolResult(BOOL_TRUE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, sessionId);
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse deleteGroup(RpcController controller, SessionedStringProto request) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        SessionIdProto sessionId = request.getSessionId();
        if (!userManager.isAdmin(sessionId.getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          userManager.deleteGroup(request.getValue());
          builder.setBoolResult(BOOL_TRUE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse changeGroupUsers(RpcController controller,
                                           GroupUserRequest request) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        SessionIdProto sessionId = request.getSessionId();
        if (!userManager.isAdmin(sessionId.getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          if (request.getRemove()) {
            userManager.removeUsersFromGroup(request.getGroupName(), request.getUserIdsList());
          } else {
            userManager.addUsersToGroup(request.getGroupName(), request.getUserIdsList());
          }
          builder.setBoolResult(BOOL_TRUE);
          builder.setResultCode(ResultCode.OK);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, request.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse addProxyUser(RpcController controller, ProxyUserProto user) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        SessionIdProto sessionId = user.getSessionId();
        if (!userManager.isAdmin(sessionId.getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          boolean success = userManager.addUser(user);
          if (success) {
            builder.setBoolResult(BOOL_TRUE);
            builder.setResultCode(ResultCode.OK);
          } else {
            builder.setResultCode(ResultCode.ERROR);
            builder.setBoolResult(BOOL_FALSE);
            builder.setErrorMessage(user.getUserId() + " already exists.");
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, user.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ServerResponse deleteProxyUser(RpcController controller, ProxyUserProto user) throws ServiceException {
      ServerResponse.Builder builder = ServerResponse.newBuilder();
      try {
        SessionIdProto sessionId = user.getSessionId();
        if (!userManager.isAdmin(sessionId.getId())) {
          builder.setResultCode(ResultCode.ERROR);
          builder.setBoolResult(BOOL_FALSE);
          builder.setErrorMessage("current user is not admin.");
        } else {
          boolean success = userManager.deleteUser(user);
          if (success) {
            builder.setBoolResult(BOOL_TRUE);
            builder.setResultCode(ResultCode.OK);
          } else {
            builder.setResultCode(ResultCode.ERROR);
            builder.setBoolResult(BOOL_FALSE);
            builder.setErrorMessage(user.getUserId() + " not exists.");
          }
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, user.getSessionId());
        builder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          builder.setErrorMessage(e.getMessage());
        }
        builder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      return builder.build();
    }

    @Override
    public ProxyUserList listProxyUsers(RpcController controller, SessionIdProto sessionId) throws ServiceException {
      ServerResponse.Builder responseBuilder = ServerResponse.newBuilder();

      List<ProxyUserProto> userList = new ArrayList<ProxyUserProto>();
      try {
        if (!userManager.isAdmin(sessionId.getId())) {
          responseBuilder.setResultCode(ResultCode.ERROR);
          responseBuilder.setBoolResult(BOOL_FALSE);
          responseBuilder.setErrorMessage("current user is not admin.");
        } else {
          Collection<ProxyUser> users = userManager.getProxyUsers();

          for (ProxyUser eachUser: users) {
            userList.add(eachUser.getProto(sessionId));
          }
          responseBuilder.setResultCode(ResultCode.OK);
          responseBuilder.setBoolResult(BOOL_TRUE);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, sessionId);
        responseBuilder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          responseBuilder.setErrorMessage(e.getMessage());
        }
        responseBuilder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      ProxyUserList.Builder builder = ProxyUserList.newBuilder();
      builder.addAllUsers(userList);
      builder.setResponse(responseBuilder.build());

      return builder.build();
    }

    @Override
    public ProxyGroupList listProxyGroups(RpcController controller, SessionIdProto sessionId) throws ServiceException {
      ServerResponse.Builder responseBuilder = ServerResponse.newBuilder();

      List<ProxyGroupProto> groupList = new ArrayList<ProxyGroupProto>();
      try {
        if (!userManager.isAdmin(sessionId.getId())) {
          responseBuilder.setResultCode(ResultCode.ERROR);
          responseBuilder.setBoolResult(BOOL_FALSE);
          responseBuilder.setErrorMessage("current user is not admin.");
        } else {
          Collection<ProxyGroup> groups = userManager.getProxyGroups();

          for (ProxyGroup eachGroup: groups) {
            groupList.add(eachGroup.getProto());
          }
          responseBuilder.setResultCode(ResultCode.OK);
          responseBuilder.setBoolResult(BOOL_TRUE);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        checkTajoInvalidSession(e, sessionId);
        responseBuilder.setResultCode(ResultCode.ERROR);
        if (e.getMessage() != null) {
          responseBuilder.setErrorMessage(e.getMessage());
        }
        responseBuilder.setDetailErrorMessage(StringUtils.stringifyException(e));
      }

      ProxyGroupList.Builder builder = ProxyGroupList.newBuilder();
      builder.addAllGroups(groupList);
      builder.setResponse(responseBuilder.build());

      return builder.build();
    }

    @Override
    public ServerResponse grantTableToGroup(RpcController controller,
                                            GrantTableRequest request) throws ServiceException {
      ServerResponse.Builder responseBuilder = ServerResponse.newBuilder();
      if (!userManager.isAdmin(request.getSessionId().getId())) {
        responseBuilder.setResultCode(ResultCode.ERROR);
        responseBuilder.setBoolResult(BOOL_FALSE);
        responseBuilder.setErrorMessage("current user is not admin.");
        return responseBuilder.build();
      }

      if (request.getGrant()) {
        userManager.addTables(request.getGroupName(), request.getTableNamesList());
      } else {
        userManager.removeTables(request.getGroupName(), request.getTableNamesList());
      }
      responseBuilder.setResultCode(ResultCode.OK);
      responseBuilder.setBoolResult(BOOL_TRUE);
      return responseBuilder.build();
    }

    @Override
    public ServerResponse changePassword(RpcController controller,
                                            ChangePasswordRequest request) throws ServiceException {
      ServerResponse.Builder responseBuilder = ServerResponse.newBuilder();
      if (!userManager.isAdmin(request.getSessionId().getId())) {
        responseBuilder.setResultCode(ResultCode.ERROR);
        responseBuilder.setBoolResult(BOOL_FALSE);
        responseBuilder.setErrorMessage("current user is not admin.");
        return responseBuilder.build();
      }

      userManager.changePassword(request.getUserId(), request.getPassword());
      responseBuilder.setResultCode(ResultCode.OK);
      responseBuilder.setBoolResult(BOOL_TRUE);
      return responseBuilder.build();
    }

//    @Override
//    public QueryInfoList listUserQueries(RpcController controller,
//                                         QueryInfoListRequest request) throws ServiceException {
//      QueryInfoList.Builder builder = QueryInfoList.newBuilder();
//
//      //QueryId, Query Master, Started, Progress, Time, Status, sql
//      List<QueryInfoProto> queryInfos = new ArrayList<QueryInfoProto>();
//      QueryInfoProto.Builder queryInfoBuilder = QueryInfoProto.newBuilder();
//      synchronized (querySubmitTasks) {
//        for (QuerySubmitTask eachQuery: querySubmitTasks.values()) {
//          if (request.hasUserId() && !request.getUserId().isEmpty()) {
//            String queryUserId = eachQuery.queryProgressInfo.getRealUserId();
//            if (!queryUserId.equals(request.getUserId())) {
//              continue;
//            }
//          }
//          queryInfoBuilder.clear();
//          queryInfoBuilder.setUserId(eachQuery.queryProgressInfo.userId);
//          if (eachQuery.queryProgressInfo.externalParam != null) {
//            queryInfoBuilder.setExternalUserId(eachQuery.queryProgressInfo.externalParam.userId);
//            queryInfoBuilder.setExternalQueryId(eachQuery.queryProgressInfo.externalParam.getQueryId());
//          }
//          queryInfoBuilder.setQueryStatus(eachQuery.queryProgressInfo.queryStatus);
//          queryInfoBuilder.setQuery(eachQuery.queryProgressInfo.query);
//          queryInfos.add(queryInfoBuilder.build());
//        }
//      }
//      synchronized(finishedQueries) {
//        for(QueryProgressInfo eachQuery: finishedQueries) {
//          if (request.hasUserId() && !request.getUserId().isEmpty()) {
//            String queryUserId = eachQuery.getRealUserId();
//            if (!queryUserId.equals(request.getUserId())) {
//              continue;
//            }
//          }
//          queryInfoBuilder.clear();
//          queryInfoBuilder.setUserId(eachQuery.userId);
//          if (eachQuery.externalParam != null) {
//            queryInfoBuilder.setExternalUserId(eachQuery.externalParam.userId);
//            queryInfoBuilder.setExternalQueryId(eachQuery.externalParam.getQueryId());
//          }
//          queryInfoBuilder.setQueryStatus(eachQuery.queryStatus);
//          queryInfoBuilder.setQuery(eachQuery.query);
//          queryInfos.add(queryInfoBuilder.build());
//        }
//      }
//      return builder.addAllQueries(queryInfos).build();
//    }

    @Override
    public ServerResponse refreshSession(RpcController controller, SessionIdProto sessionId) throws ServiceException {
      synchronized(tajoClientMap) {
        TajoClientHolder clientHolder = tajoClientMap.get(sessionId);
        if (clientHolder != null) {
          clientHolder.lastTouchTime = System.currentTimeMillis();
        }
      }
      ServerResponse.Builder responseBuilder = ServerResponse.newBuilder();
      responseBuilder.setResultCode(ResultCode.OK);
      responseBuilder.setBoolResult(BOOL_TRUE);
      return responseBuilder.build();
    }
  }

  private void removeQueryTask(TajoIdProtos.SessionIdProto sessionId, QueryId queryId) {
    synchronized (queryResultSets) {
      queryResultSets.remove(ResultSetHolder.getKey(sessionId, queryId));
    }

    synchronized (querySubmitTasks) {
      QuerySubmitTask task = querySubmitTasks.remove(ResultSetHolder.getKey(sessionId, queryId));
//      if (task != null) {
//        synchronized (finishedQueries) {
//          finishedQueries.add(task.queryProgressInfo.clone());
//        }
//      }
    }
  }

  class QuerySubmitTask extends Thread {
    QueryProgressInfo queryProgressInfo;
    AtomicBoolean stopFlag = new AtomicBoolean(false);

    public QuerySubmitTask(TajoIdProtos.SessionIdProto sessionId) {
      super("QuerySubmitTask");
      queryProgressInfo = new QueryProgressInfo(sessionId);
    }

    private String getKey() {
      return ResultSetHolder.getKey(queryProgressInfo.sessionId, queryProgressInfo.queryId);
    }

    public void stopTask() {
      stopFlag.set(true);
      this.interrupt();
    }

    @Override
    public void run() {
      LOG.info("QuerySubmitTask started for " + queryProgressInfo.sessionId + "," + queryProgressInfo.queryId);
      GetQueryStatusRequest.Builder builder = GetQueryStatusRequest.newBuilder();
      builder.setQueryId(queryProgressInfo.queryId.getProto());
      queryProgressInfo.statusRequest = builder.build();

      GetQueryStatusResponse lastResponse = null;
      boolean queryFinished = false;
      int errorCount = 0;
      while (!stopFlag.get()) {
        lastResponse = getQueryStatusInternal(queryProgressInfo.statusRequest);
        if (lastResponse == null) {
          queryProgressInfo.queryStatus = createErrorResponse(queryProgressInfo.queryId,
              "No query submit info from TajoMaster for " + queryProgressInfo.queryId);
          break;
        }
        if (!TajoProxyClient.isQueryRunnning(lastResponse.getState())) {
          queryFinished = true;
          break;
        }
        queryProgressInfo.queryStatus = lastResponse;

        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
        if (lastResponse.getResultCode() == ResultCode.ERROR) {
          errorCount++;
          // If error count > 3, query failed.
          if (errorCount > 3) {
            break;
          }
        } else {
          errorCount = 0;
        }
      }
      if (errorCount > 3) {
        LOG.error("QuerySubmitTask stopped for " + queryProgressInfo.sessionId + "," +
            queryProgressInfo.queryId + ", cause " + lastResponse.getErrorMessage());
        queryProgressInfo.queryStatus = createErrorResponse(
            queryProgressInfo.queryId, "QuerySubmitTask stopped for " + queryProgressInfo.sessionId + "," +
                queryProgressInfo.queryId + ", cause " + lastResponse.getErrorMessage());
        return;
      }

      if (stopFlag.get()) {
        LOG.info("QuerySubmitTask be forced to stop [" + queryProgressInfo.queryId + "]");
        Builder responseBuilder = GetQueryStatusResponse.newBuilder();
        responseBuilder.setQueryId(queryProgressInfo.queryId.getProto());
        responseBuilder.setResultCode(ResultCode.OK);
        responseBuilder.setState(TajoProtos.QueryState.QUERY_KILLED);
        responseBuilder.setErrorMessage("QuerySubmitTask be forced to stop [" + queryProgressInfo.queryId + "]");
        queryProgressInfo.queryStatus = responseBuilder.build();
        return;
      }
      //get result
      ResultSetHolder resultSetHolder = new ResultSetHolder();
      try {
        TajoResultSet rs = null;
        if (queryFinished && lastResponse.getState() == TajoProtos.QueryState.QUERY_SUCCEEDED) {
          if (lastResponse.getHasResult()) {
            rs = (TajoResultSet)getTajoClient(queryProgressInfo.sessionId).getQueryResult(queryProgressInfo.queryId);
          } else {
            rs = (TajoResultSet)createNullResultSet(getTajoClient(queryProgressInfo.sessionId), queryProgressInfo.queryId);
          }
        } else {
          LOG.warn("Query (" + queryProgressInfo.sessionId.getId() + "," +
              queryProgressInfo.queryStatus.getQueryId() + ") failed: " + queryProgressInfo.queryStatus.getState());
          rs = (TajoResultSet)createNullResultSet(getTajoClient(queryProgressInfo.sessionId), queryProgressInfo.queryId);
        }
        resultSetHolder.rs = rs;
        resultSetHolder.tableDesc = rs.getTableDesc();
        resultSetHolder.lastTouchTime = System.currentTimeMillis();

        synchronized (queryResultSets) {
          LOG.info("Query completed: " + ResultSetHolder.getKey(queryProgressInfo.sessionId, queryProgressInfo.queryId));
          queryResultSets.put(ResultSetHolder.getKey(queryProgressInfo.sessionId, queryProgressInfo.queryId), resultSetHolder);
        }
        queryProgressInfo.queryStatus = lastResponse;
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);

        Builder responseBuilder = GetQueryStatusResponse.newBuilder();
        responseBuilder.setQueryId(queryProgressInfo.statusRequest.getQueryId());
        responseBuilder.setResultCode(ResultCode.ERROR);
        responseBuilder.setState(TajoProtos.QueryState.QUERY_ERROR);
        responseBuilder.setErrorMessage("Can't get query result for " + queryProgressInfo.queryId + " cause:\n" +
            StringUtils.stringifyException(e));

        queryProgressInfo.queryStatus = responseBuilder.build();
      }

      LOG.info("QuerySubmitTask stopped for " + queryProgressInfo.sessionId + "," + queryProgressInfo.queryId + "," +
          queryProgressInfo.queryStatus.getResultCode());
    }

    private GetQueryStatusResponse getQueryStatusInternal(GetQueryStatusRequest request) {
      Builder responseBuilder = GetQueryStatusResponse.newBuilder();
      responseBuilder.setQueryId(request.getQueryId());

      try {
        QueryStatus queryStatus = getTajoClient(queryProgressInfo.sessionId).getQueryStatus(new QueryId(request.getQueryId()));
        ResultCode resultCode = ResultCode.OK;

        if (queryStatus.getErrorMessage() != null) {
          resultCode = ResultCode.ERROR;
        }
        responseBuilder.setResultCode(resultCode);
        responseBuilder.setState(queryStatus.getState());
        if (queryStatus.getErrorMessage() != null) {
          responseBuilder.setErrorMessage(queryStatus.getErrorMessage());
        }
        responseBuilder.setProgress(queryStatus.getProgress());
        responseBuilder.setFinishTime(queryStatus.getFinishTime());
        responseBuilder.setHasResult(queryStatus.hasResult());

        if (queryStatus.getQueryMasterHost() != null) {
          responseBuilder.setQueryMasterHost(queryStatus.getQueryMasterHost());
        }
        responseBuilder.setQueryMasterPort(queryStatus.getQueryMasterPort());
        responseBuilder.setSubmitTime(queryStatus.getSubmitTime());
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        responseBuilder.setResultCode(ResultCode.ERROR);
        responseBuilder.setErrorMessage(StringUtils.stringifyException(e));
        checkTajoInvalidSession(e, queryProgressInfo.sessionId);
      }

      return responseBuilder.build();
    }
  }

  private void checkTajoInvalidSession(Throwable e, TajoIdProtos.SessionIdProto sessionId) {
    if (e.getMessage() != null && e.getMessage().indexOf("InvalidSessionException") >= 0) {
      synchronized (tajoClientMap) {
        tajoClientMap.remove(sessionId);
        userManager.logout(sessionId.getId());
      }
    }
  }

  class ResultSetAndTaskCleaner extends Thread {
    int resultSetExpireInterval;
    int queryTaskExpireInterval;
    int sessionTaskExpireInterval;
    int sessionExpireInterval;

    public ResultSetAndTaskCleaner() {
      super("ResultSetAndTaskCleaner");
    }

    @Override
    public void run() {
      resultSetExpireInterval = tajoConf.getInt("tajo-proxy.proxyserver.resultset.expire.interval.sec", 120);
      queryTaskExpireInterval = tajoConf.getInt("tajo-proxy.proxyserver.querytask.expire.interval.sec", 120);
      sessionTaskExpireInterval = tajoConf.getInt("tajo-proxy.proxyserver.session.expire.interval.sec",
          12 * 60 * 60);   //12 hour
      sessionExpireInterval = tajoConf.getInt("tajo-proxy.proxyserver.session.expire.interval.sec", 24 * 60 * 60);

      LOG.info("ResultSetAndTaskCleaner started: resultSetExpireInterval=" + resultSetExpireInterval + "," +
          "queryTaskExpireInterval=" + queryTaskExpireInterval + ",sessionExpireInterval=" + sessionExpireInterval);

      while (true) {
        try {
          cleanQueryResult();
          cleanQueryTask();
          cleanTajoClient();
          cleanSession();
          Thread.sleep(60 * 1000);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    private void cleanSession() {
      Map<SessionIdProto, TajoClientHolder> cleanCheckList = new HashMap<SessionIdProto, TajoClientHolder>();
      synchronized(tajoClientMap) {
        cleanCheckList.putAll(tajoClientMap);
      }

      for (Map.Entry<SessionIdProto, TajoClientHolder> entry: cleanCheckList.entrySet()) {
        SessionIdProto sessionId = entry.getKey();
        TajoClientHolder eachTajoClient = entry.getValue();
        long timeGap = System.currentTimeMillis() - eachTajoClient.lastTouchTime;
        if (timeGap > sessionExpireInterval * 1000) {
          synchronized (tajoClientMap) {
            tajoClientMap.remove(sessionId);
          }

          userManager.logout(sessionId.getId());
        }
      }
    }

    private void cleanQueryResult() {
      List<ResultSetHolder> queryResultSetCleanList = new ArrayList<ResultSetHolder>();

      synchronized (queryResultSets) {
        queryResultSetCleanList.addAll(queryResultSets.values());
      }

      for (ResultSetHolder eachResultSetHolder: queryResultSetCleanList) {
        if (System.currentTimeMillis() - eachResultSetHolder.lastTouchTime > resultSetExpireInterval * 1000) {
          try {
            if (!eachResultSetHolder.rs.isClosed()) {
              LOG.info("ResultSet close:" + eachResultSetHolder.queryId);
              eachResultSetHolder.rs.close();

              synchronized (queryResultSets) {
                queryResultSets.remove(eachResultSetHolder.getKey());
              }
            }
          } catch (SQLException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }

    private void cleanQueryTask() {
      List<QuerySubmitTask> querySubmitTaskCleanList = new ArrayList<QuerySubmitTask>();
      synchronized (querySubmitTasks) {
        querySubmitTaskCleanList.addAll(querySubmitTasks.values());
      }

      for (QuerySubmitTask eachTask: querySubmitTaskCleanList) {
        if (System.currentTimeMillis() - eachTask.queryProgressInfo.lastTouchTime > queryTaskExpireInterval * 1000) {
          eachTask.stopTask();
          synchronized (querySubmitTasks) {
            QuerySubmitTask task = querySubmitTasks.remove(eachTask.queryProgressInfo.queryId);
//            if (task != null) {
//              synchronized (finishedQueries) {
//                finishedQueries.add(task.queryProgressInfo.clone());
//              }
//            }
          }
        }
      }
    }

    private void cleanTajoClient() {
      List<TajoIdProtos.SessionIdProto> tajoClientCleanList = new ArrayList<TajoIdProtos.SessionIdProto>();
      synchronized (tajoClientMap) {
        tajoClientCleanList.addAll(tajoClientMap.keySet());
      }

      for (TajoIdProtos.SessionIdProto eachSessionId: tajoClientCleanList) {
        TajoClientHolder tajoClientHolder = tajoClientMap.get(eachSessionId);
        if (tajoClientHolder == null) {
          continue;
        }
        if (System.currentTimeMillis() - tajoClientHolder.lastTouchTime > sessionTaskExpireInterval * 1000) {
          synchronized (tajoClientMap) {
            tajoClientMap.remove(eachSessionId);
          }
          tajoClientHolder.tajoClient.close();
        }
      }
    }
  }
}