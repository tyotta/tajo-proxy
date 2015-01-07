package org.apache.tajo.jdbc.proxy;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.TajoIdProtos.SessionIdProto;
import org.apache.tajo.TajoProtos.QueryState;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.*;
import org.apache.tajo.jdbc.TajoMemoryResultSet;
import org.apache.tajo.proxy.*;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol.*;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.ServerCallable;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@ThreadSafe
public class TajoProxyClient implements TajoClientInterface {
  public static final String TAJO_PROXY_SERVERS = "tajo.proxy.servers";

  private static final Log LOG = LogFactory.getLog(TajoProxyClient.class);

  //TODO configuable
  public static final int DEFAULT_FETCH_SIZE = 1000;

  public static final int DEFAULT_MAX_ROWS = 200000;

  private List<InetSocketAddress> proxyServers = new ArrayList<InetSocketAddress>();
  
  private Set<InetSocketAddress> dirtyServers = new HashSet<InetSocketAddress>();
  
  private RpcConnectionPool rpcPool;
  
  private final Random rand;

  private String databaseName;

  private Map<InetSocketAddress, SessionIdProto> sessionIdMap =
      new HashMap<InetSocketAddress, SessionIdProto>();

  private SessionIdProto lastSessionId;

  private String characterEncoding;

  private Map<QueryId, InetSocketAddress> queryProxyServerMap = new HashMap<QueryId, InetSocketAddress>();

  private TajoConf tajoConf;

  private String currentUser;

  private AtomicBoolean closed = new AtomicBoolean(false);

  private SessionRefreshThread sessionRefreshThread;

  private Map<String, String> sessionVariables = new HashMap<String, String>();

  public TajoProxyClient(TajoConf conf, List<String> proxyServers, String databaseName) throws IOException {
    this(conf, proxyServers, databaseName, null);
  }

  public TajoProxyClient(TajoConf conf, List<String> proxyServers, String databaseName,
                         String characterEncoding) throws IOException {
    if (proxyServers == null || proxyServers.isEmpty()) {
      throw new IOException("No proxy server");
    }
    this.tajoConf = conf;
    this.characterEncoding = characterEncoding;

    for (String eachProxyServer: proxyServers) {
      String [] splitted = eachProxyServer.split(":");
      if (splitted.length != 2) {
        LOG.warn("Wrong proxyserver address[" + eachProxyServer + "]");
      }
      this.proxyServers.add(new InetSocketAddress(splitted[0], Integer.parseInt(splitted[1])));
    }
    
    if (this.proxyServers.isEmpty()) {
      throw new IOException("No valid proxy server");
    }

    this.databaseName = databaseName == null ? "default" : databaseName;

    this.rand = new Random(System.currentTimeMillis());

    rpcPool = RpcConnectionPool.newPool(conf, getClass().getSimpleName(),
        conf.getInt("tajo-proxy.jdbc.rpc.workers", 5));
  }

  public TajoIdProtos.SessionIdProto getLastSessionId() {
    return lastSessionId;
  }

  public void setCurrentUser(String currentUser) {
    this.currentUser = currentUser;
  }

  public String getCurrentUser() {
    return currentUser;
  }

  public TajoConf getConf() {
    return tajoConf;
  }

  @Override
  public boolean isConnected() {
    try {
      return getProxyServer(true).isConnected();
    } catch (Exception e) {
      return false;
    }
  }

  public List<InetSocketAddress> getProxyServers() {
    return proxyServers;
  }

  private InetSocketAddress selectProxyServer() throws IOException {
    if (this.proxyServers.isEmpty() || this.proxyServers.size() == this.dirtyServers.size()) {
      throw new IOException("Can't find live proxy server: "
          + "# ProxyServer=" + proxyServers.size() + ", # DirtyServer = " + dirtyServers.size());
    }
    int retryCount = 0;

    while (true) {
      InetSocketAddress selectedProxyServer = this.proxyServers.get(rand.nextInt(proxyServers.size()));
      if (!dirtyServers.contains(selectedProxyServer)) {
        return selectedProxyServer;
      }
      retryCount++;
      if (retryCount > 10) {
        throw new IOException("Can't find live proxy server: "
            + "# ProxyServer=" + proxyServers.size() + ", # DirtyServer = " + dirtyServers.size());
      }
    }
  }

  private NettyClientBase getProxyServer(boolean asyncMode) throws IOException {
    InetSocketAddress selectedProxyServer = selectProxyServer();
    try {
      return rpcPool.getConnection(selectedProxyServer, ProxyServerClientProtocol.class, asyncMode);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  private NettyClientBase getProxyServer(QueryId queryId, boolean asyncMode) throws IOException {
    InetSocketAddress selectedProxyServer = null;
    synchronized (queryProxyServerMap) {
      selectedProxyServer = queryProxyServerMap.get(queryId);
    }
    if (selectedProxyServer == null) {
      throw new IOException("Can't find tajo proxy server for query:" + queryId);
    }
    try {
      return rpcPool.getConnection(selectedProxyServer, ProxyServerClientProtocol.class, asyncMode);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet createNullResultSet(QueryId queryId) throws IOException {
    return new TajoResultSet(this, queryId, characterEncoding,
        ProxyQueryResult.newBuilder()
            .setQueryStatus(GetQueryStatusResponse.newBuilder()
                .setResultCode(ResultCode.OK).setQueryId(queryId.getProto()).build())
            .addAllRows(Collections.<ByteString>emptyList())
            .build());
  }
  
  @Override
  public void closeQuery(QueryId queryId) {
    NettyClientBase proxyClient = null;
    try {
      proxyClient = getProxyServer(queryId, false);
      ProxyServerClientProtocolService.BlockingInterface proxyService = proxyClient.getStub();
      checkSessionAndGet(proxyClient.getRemoteAddress(), proxyService);

      proxyService.closeQuery(null,
          ProxyServerClientProtocol.CloseQueryRequest.newBuilder()
            .setQueryId(queryId.getProto())
            .setSessionId(sessionIdMap.get(proxyClient.getRemoteAddress()))
            .build());
    } catch (Exception e) {
      LOG.warn("Fail to close query (qid=" + queryId + ", msg=" + e.getMessage() + ")", e);
    }
  }

  private String password;

  public boolean login(final String userId, final String passwd, final boolean passwdEncoded) throws IOException {
    setCurrentUser(userId);
    if (!passwdEncoded) {
      try {
        this.password = ProxyUserAdmin.encodingPassword(passwd);
      } catch (Exception e) {
        throw new IOException(e.getMessage(), e);
      }
    } else {
      this.password = passwd;
    }

    InetSocketAddress proxyServer = selectProxyServer();
    return doLogin(userId, this.password, proxyServer);
  }

  @Override
  public boolean login(final String userId, final String passwd) throws IOException {
    String encodedPassword = null;
    try {
      encodedPassword = ProxyUserAdmin.encodingPassword(passwd);
    } catch (Exception e) {
      throw new IOException (e.getMessage(), e);
    }

    return login(userId, encodedPassword, true);
  }

  private boolean doLogin(final String userId, final String encodedPasswd,
                         final InetSocketAddress proxyServer) throws IOException {
    try {
      Boolean result =  new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();

          ServerResponse response = null;
          try {
            response = callLogin(proxyService, userId, encodedPasswd);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            SessionIdProto serverSessionId = response.getSessionId();
            synchronized (sessionIdMap) {
              sessionIdMap.put(proxyServer, serverSessionId);
            }
            return true;
          } else {
            //LOG.error(response.getErrorMessage());
            abort();
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();

      if (result) {
        sessionRefreshThread = new SessionRefreshThread();
        sessionRefreshThread.start();
      }

      return result;
    } catch (Exception e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  private ServerResponse callLogin(ProxyServerClientProtocolService.BlockingInterface proxyService,
                           String userId, String encodedPasswd) throws IOException {
    if (userId == null || userId.isEmpty() || encodedPasswd == null || encodedPasswd.isEmpty()) {
      throw new IOException("Not login user");
    }
    CreateProxySessionRequest.Builder builder = CreateProxySessionRequest.newBuilder();
    builder.setUser(ProxyUserProto.newBuilder().setUserId(userId).setPassword(encodedPasswd).build());
    builder.setDefaultDatabase(databaseName);
    try {
      return proxyService.createSession(null, builder.build());
    } catch (ServiceException e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  private synchronized SessionIdProto checkSessionAndGet(final InetSocketAddress proxyServer,
    ProxyServerClientProtocolService.BlockingInterface proxyService) throws IOException {
    SessionIdProto serverSessionId = null;
    synchronized (sessionIdMap) {
      serverSessionId = sessionIdMap.get(proxyServer);
    }
    if (serverSessionId == null) {
      ServerResponse response = callLogin(proxyService, currentUser, password);
      if (response.getResultCode() == ClientProtos.ResultCode.OK) {
        serverSessionId = response.getSessionId();
        synchronized(sessionIdMap) {
          sessionIdMap.put(proxyServer, serverSessionId);
        }
      } else {
        LOG.error(response.getErrorMessage());
        throw new IOException(response.getErrorMessage());
      }
    }
    return serverSessionId;
  }

  @Override
  public GetQueryStatusResponse executeQuery(final String sql) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      GetQueryStatusResponse response = new ProxyClientServerCallable<GetQueryStatusResponse>(proxyServer) {
        public GetQueryStatusResponse call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          final QueryRequest.Builder builder = QueryRequest.newBuilder();
          builder.setSessionId(sessionId);
          builder.setQuery(sql);
          builder.setIsJson(false);
          GetQueryStatusResponse response = null;
          try {
            response = proxyService.submitQuery(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          return response;
        }
      }.withRetries();

      if (response != null && response.hasQueryId()) {
        QueryId queryId = new QueryId(response.getQueryId());
        synchronized (queryProxyServerMap) {
          queryProxyServerMap.put(queryId, proxyServer);
        }
      }
      return response;
    } catch (Exception e) {
      GetQueryStatusResponse.Builder builder = GetQueryStatusResponse.newBuilder();
      builder.setErrorMessage(e.getMessage());
      builder.setErrorTrace(StringUtils.stringifyException(e));
      builder.setResultCode(ResultCode.ERROR);
      builder.setState(QueryState.QUERY_ERROR);

      return builder.build();
    }
  }

  @Override
  public ResultSet executeQueryAndGetResult(final String sql) throws IOException {
    return executeQueryAndGetResult(sql, DEFAULT_FETCH_SIZE);
  }

  public ResultSet executeQueryAndGetResult(final String sql, int fetchSize) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      GetQueryStatusResponse response = new ProxyClientServerCallable<GetQueryStatusResponse>(proxyServer) {
        public GetQueryStatusResponse call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          final QueryRequest.Builder builder = QueryRequest.newBuilder();

          builder.setQuery(sql);
          builder.setSessionId(sessionId);
          builder.setIsJson(false);
          GetQueryStatusResponse response = null;
          try {
            response = proxyService.submitQuery(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          if (response.getResultCode() != ResultCode.OK || response.hasErrorMessage()) {
            abort();
            throw new IOException(response.getErrorMessage());
          }
          return response;
        }
      }.withRetries();

      if (response != null && response.hasQueryId()) {
        QueryId queryId = new QueryId(response.getQueryId());
        synchronized (queryProxyServerMap) {
          queryProxyServerMap.put(queryId, proxyServer);
        }
        return this.getQueryResultAndWait(queryId, fetchSize);
      }
      return this.createNullResultSet(QueryIdFactory.NULL_QUERY_ID);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public ResultSet getQueryResultAndWait(QueryId queryId, int fetchSize) throws IOException {
    QueryStatus status = getQueryStatus(queryId);
    while(status != null && isQueryRunnning(status.getState())) {
      try {
        //TODO use thread
        Thread.sleep(100);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      status = getQueryStatus(queryId);
    }
    if (status.getState() == QueryState.QUERY_SUCCEEDED) {
      if (status.hasResult()) {
        return getQueryResult(queryId, fetchSize);
      } else {
        return createNullResultSet(queryId);
      }
    } else {
      LOG.warn("Query (" + status.getQueryId() + ") failed: " + status.getState());
      //TODO change SQLException
      throw new IOException("Query (" + status.getQueryId() + ") failed: " + status.getState() + 
          " cause " + status.getErrorMessage());
    }
  }
  
  public static boolean isQueryRunnning(QueryState state) {
    return state == QueryState.QUERY_NEW ||
        state == QueryState.QUERY_RUNNING ||
        state == QueryState.QUERY_MASTER_LAUNCHED ||
        state == QueryState.QUERY_MASTER_INIT ||
        state == QueryState.QUERY_NOT_ASSIGNED;
  }

  public static boolean isQueryRunnning(String stateName) {
    return stateName.equals(QueryState.QUERY_NEW.name()) ||
        stateName.equals(QueryState.QUERY_RUNNING.name())  ||
        stateName.equals(QueryState.QUERY_MASTER_LAUNCHED.name())  ||
        stateName.equals(QueryState.QUERY_MASTER_INIT.name())  ||
        stateName.equals(QueryState.QUERY_NOT_ASSIGNED.name()) ;
  }

  @Override
  public QueryStatus getQueryStatus(QueryId queryId) throws IOException {
    GetQueryStatusResponse res = null;
    NettyClientBase proxyClient = null;
    try {
      proxyClient = getProxyServer(queryId, false);
      ProxyServerClientProtocolService.BlockingInterface proxyService = proxyClient.getStub();
      SessionIdProto sessionId = checkSessionAndGet(proxyClient.getRemoteAddress(), proxyService);

      GetQueryStatusRequest.Builder builder = GetQueryStatusRequest.newBuilder();
      builder.setQueryId(queryId.getProto());
      builder.setSessionId(sessionId);
      res = proxyService.getQueryStatus(null, builder.build());
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    } finally {
      rpcPool.releaseConnection(proxyClient);
    }
    return new QueryStatus(res);
  }

  @Override
  public ResultSet getQueryResult(QueryId queryId) throws IOException {
    return getQueryResult(queryId, DEFAULT_FETCH_SIZE);
  }

  @Override
  public ResultSet getQueryResult(QueryId queryId, int fetchSize) throws IOException {
    ProxyQueryResult queryResult = getProxyQueryResult(queryId, fetchSize);
    ResultSet resultSet = null;
    if (queryResult.hasSchema()) {
      resultSet = new TajoMemoryResultSet(new Schema(queryResult.getSchema()),
        queryResult.getRowsList(), queryResult.getRowsCount(), null);
    } else {
      resultSet = new TajoResultSet(this, queryId, characterEncoding, queryResult);
    }
    try {
      resultSet.setFetchSize(fetchSize);
    } catch (SQLException e) {
    }

    return resultSet;
  }

  @Override
  public ProxyQueryResult getProxyQueryResult(final QueryId queryId, int fetchSize) throws IOException {
    NettyClientBase proxyClient = null;
    try {
      proxyClient = getProxyServer(queryId, false);
      ProxyServerClientProtocolService.BlockingInterface proxyService = proxyClient.getStub();
      SessionIdProto sessionId = checkSessionAndGet(proxyClient.getRemoteAddress(), proxyService);

      GetQueryResultRequest resultRequestMeta = GetQueryResultRequest.newBuilder()
          .setSessionId(sessionId)
          .setQueryId(queryId.getProto()).build();

      ProxyQueryResultRequest.Builder builder = ProxyQueryResultRequest.newBuilder()
          .setResultRequestMeta(resultRequestMeta)
          .setFetchSize(fetchSize);

      return proxyService.getQueryResult(null, builder.build());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new IOException(e.getMessage(), e);
    } finally {
      rpcPool.releaseConnection(proxyClient);
    }
  }

  @Override
  public boolean existTable(final String tableName) throws IOException {
    try {
      return new ProxyClientServerCallable<Boolean>(selectProxyServer()) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          ProxyServerClientProtocol.ServerResponse response = null;
          try {
            response = proxyService.existTable(null,
                SessionedStringProto.newBuilder()
                    .setSessionId(sessionId)
                    .setValue(tableName)
                    .build());
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public boolean dropTable(final String tableName) throws IOException {
    return dropTable(tableName, false);
  }

  @Override
  public boolean dropTable(final String tableName, final boolean purge) throws IOException {
    try {
      return new ProxyClientServerCallable<Boolean>(selectProxyServer()) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          ClientProtos.DropTableRequest dropTableRequest =
              ClientProtos.DropTableRequest.newBuilder()
                  .setName(tableName)
                  .setPurge(purge)
                  .setSessionId(sessionId)
                  .build();

          ProxyServerClientProtocol.ServerResponse response = null;
          try {
            response = proxyService.dropTable(null, dropTableRequest);
          } catch (Throwable t) {
            throw new IOException (t.getMessage(), t);
          }
          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public QueryHistory getQueryHsitory(final String queryId) throws IOException {
    List<QueryHistory> queries = listQueryHistory("");
    if (queries == null || queries.isEmpty()) {
      return null;
    }

    for (QueryHistory eachQuery: queries) {
      if (eachQuery.getQueryId().equals(queryId)) {
        return eachQuery;
      }
    }
    return null;
  }

  @Override
  public List<QueryHistory> listQueryHistory(final String userId) throws IOException {
    return listQueryHistory(userId, false);
  }

  @Override
  public List<QueryHistory> listQueryHistory(final String userId, final boolean proxyQueryOnly) throws IOException {
    try {
      return new ProxyClientServerCallable<List<QueryHistory>>(selectProxyServer()) {
        public List<QueryHistory> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          ClientProtos.GetQueryListRequest.Builder builder = ClientProtos.GetQueryListRequest.newBuilder();
          builder.setSessionId(sessionId);
          ClientProtos.GetQueryListResponse response;
          try {
            response = proxyService.getQueryList(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          List<QueryHistory> result = new ArrayList<QueryHistory>();
          try {
            for (BriefQueryInfo eachQuery : response.getQueryListList()) {
              if (eachQuery.hasFinishTime() && eachQuery.getFinishTime() > 0) {
                if (System.currentTimeMillis() - eachQuery.getFinishTime() > 4 * 60 * 60 * 1000) {
                  continue;
                }
              }

              QueryExternalParam externalParam = ProxyClientRpcService.paresQueryExternalParam(eachQuery.getQuery());
              if (proxyQueryOnly && externalParam == null) {
                continue;
              }
              if (userId != null && !userId.isEmpty()) {
                if (externalParam != null && userId.equals(externalParam.getUserId())) {
                  QueryHistory queryHistory = new QueryHistory(eachQuery);
                  queryHistory.setQuery(externalParam.getRealQuery());
                  queryHistory.setUserId(externalParam.getUserId());
                  queryHistory.setExternalParam(externalParam);
                  result.add(queryHistory);
                }
              } else {
                QueryHistory queryHistory = new QueryHistory(eachQuery);
                if (externalParam != null) {
                  queryHistory.setUserId(externalParam.getUserId());
                  queryHistory.setQuery(externalParam.getRealQuery());
                  queryHistory.setExternalParam(externalParam);
                } else {
                  queryHistory.setUserId(userId);
                }
                result.add(queryHistory);
              }
            }
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            abort();
            throw new IOException(e.getMessage(), e);
          }

          return result;
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public List<WorkerResourceInfo> getClusterInfo() throws IOException {
    try {
      return new ProxyClientServerCallable<List<WorkerResourceInfo>>(selectProxyServer()) {
        public List<WorkerResourceInfo> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ClientProtos.GetClusterInfoRequest.Builder builder = ClientProtos.GetClusterInfoRequest.newBuilder();
          builder.setSessionId(sessionId);
          ClientProtos.GetClusterInfoResponse res;
          try {
            res = proxyService.getClusterInfo(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          return res.getWorkerListList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public List<String> getTableList(final String databaseName) throws IOException {
    try {
      return new ProxyClientServerCallable<List<String>>(selectProxyServer()) {
        public List<String> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ClientProtos.GetTableListRequest.Builder builder = ClientProtos.GetTableListRequest.newBuilder();
          builder.setSessionId(sessionId);
          builder.setDatabaseName(databaseName == null ? TajoProxyClient.this.databaseName : databaseName);
          ClientProtos.GetTableListResponse res;
          try {
            res = proxyService.getTableList(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          return res.getTablesList();
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public TableDesc getTableDesc(final String tableName) throws IOException {
    try {
      final InetSocketAddress server = selectProxyServer();
      return new ProxyClientServerCallable<TableDesc>(server) {
        public TableDesc call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ClientProtos.GetTableDescRequest.Builder build = ClientProtos.GetTableDescRequest.newBuilder();
          build.setSessionId(sessionId);
          build.setTableName(tableName);
          ClientProtos.TableResponse res = null;
          try {
            res = proxyService.getTableDesc(null, build.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (res.getResultCode() == ClientProtos.ResultCode.OK) {
            return CatalogUtil.newTableDesc(res.getTableDesc());
          } else {
            abort();
            throw new IOException(res.getErrorMessage());
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public boolean killQuery(final QueryId queryId) throws IOException {
    try {
      return new ProxyClientServerCallable<Boolean>(selectProxyServer()) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          ProxyServerClientProtocol.ServerResponse response = null;
          try {
              response = proxyService.killQuery(null,
                  QueryIdRequest.newBuilder()
                      .setSessionId(sessionId)
                      .setQueryId(queryId.getProto())
                      .build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public List<FunctionDescProto> getFunctions(final String functionName) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<List<FunctionDescProto>>(proxyServer) {
        public List<FunctionDescProto> call(NettyClientBase client) throws IOException {
          String paramFunctionName = functionName == null ? "" : functionName;

          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          FunctionResponse functionResponse = null;
          try {
            functionResponse = proxyService.getFunctionList(null,
                SessionedStringProto.newBuilder()
                    .setSessionId(sessionId)
                    .setValue(paramFunctionName)
                    .build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (functionResponse.hasErrorMessage()) {
            abort();
            throw new IOException(functionResponse.getErrorMessage());
          } else {
            return functionResponse.getFunctionsList();
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public boolean selectDatabase(final String changingDatabaseName) throws IOException {
    try {
      final InetSocketAddress server = selectProxyServer();
      return new ProxyClientServerCallable<Boolean>(server) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          ServerResponse response = null;
          try {
            response = proxyService.selectDatabase(null,
                SessionedStringProto.newBuilder()
                    .setSessionId(sessionId)
                    .setValue(changingDatabaseName)
                    .build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          if (response.hasErrorMessage()) {
            abort();
            throw new IOException(response.getErrorMessage());
          }
          boolean result = response.getBoolResult().getValue();
          TajoProxyClient.this.databaseName = changingDatabaseName;
          return result;
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public String getCurrentDatabase() throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<String>(proxyServer) {
        public String call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          try {
            return proxyService.getCurrentDatabase(null, sessionId).getValue();
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public List<String> getAllDatabaseNames() throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<List<String>>(proxyServer) {
        public List<String> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          try {
            return proxyService.getAllDatabases(null, sessionId).getValuesList();
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public boolean createDatabase(final String databaseName) throws IOException {
//    try {
//      final InetSocketAddress proxyServer = selectProxyServer();
//      return new ProxyClientServerCallable<List<String>>(proxyServer) {
//        public List<String> call(NettyClientBase client) throws ServiceException {
//          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
//          checkSessionAndGet(proxyService);
//          return proxyService.createDatabase(null, SessionedStringProto.newBuilder()
//              .setSessionId(sessionId)
//              .setValue(databaseName)
//              .build())).getValuesList();
//        }
//      }.withRetries();
//    } catch (ServiceException e) {
//      throw new IOException (e.getMessage(), e);
//    }
    throw new IOException("create database not supported in the proxy shell");
  }

  public boolean dropDatabase(final String databaseName) throws IOException {
    throw new IOException("drop database not supported in the proxy shell");
  }

  public Boolean existDatabase(final String databaseName) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          try {
            return proxyService.existDatabase(null,
                SessionedStringProto.newBuilder()
                    .setSessionId(sessionId)
                    .setValue(databaseName)
                    .build()).getBoolResult().getValue();
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if (closed.get()) {
      return;
    }
    try {
      closeSession();
    } catch (Exception e) {
      LOG.warn(e.getMessage());
    }
    if(rpcPool != null) {
      try {
        rpcPool.shutdown();
      } catch (Exception e) {
        LOG.warn(e.getMessage(), e);
      }
    }
    closed.set(true);

    if (sessionRefreshThread != null) {
      synchronized (sessionRefreshThread) {
        sessionRefreshThread.notifyAll();
      }
    }
  }

  private boolean closeSession() throws IOException {
    for(Map.Entry<InetSocketAddress, SessionIdProto> entry: sessionIdMap.entrySet()) {
      final InetSocketAddress addr = entry.getKey();
      final SessionIdProto sessionId = entry.getValue();

      try {
        return new ProxyClientServerCallable<Boolean>(addr) {
          public Boolean call(NettyClientBase client) throws ServiceException {
            ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
            ProxyServerClientProtocol.ServerResponse response =
                proxyService.closeSession(null, sessionId);
            return true;
          }
        }.withRetries();
      } catch (ServiceException e) {
        throw new IOException(e.getMessage(), e);
      }
    }
    return true;
  }

  public Map<String, String> getAllSessionVariables() throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<Map<String, String>>(proxyServer) {
        public Map<String, String> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);
          KeyValueSet keyValueSet = null;
          try {
            keyValueSet = new KeyValueSet(proxyService.getAllSessionVariables(null, sessionId));
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
          return keyValueSet.getAllKeyValus();
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean updateSessionVariables(final Map<String, String> variables) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          KeyValueSet keyValueSet = new KeyValueSet();
          keyValueSet.putAll(variables);
          UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
              .setSessionId(sessionId)
              .setSetVariables(keyValueSet.getProto()).build();

          try {
            return proxyService.updateSessionVariables(null, request).getValue();
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean unsetSessionVariables(final List<String> variables)  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          UpdateSessionVariableRequest request = UpdateSessionVariableRequest.newBuilder()
              .setSessionId(sessionId)
              .addAllUnsetVariables(variables).build();

          try {
            return proxyService.updateSessionVariables(null, request).getValue();
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean addUser(final ProxyUser proxyUser)  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ServerResponse response = null;
          try {
            response = proxyService.addProxyUser(null, proxyUser.getProto(sessionId));
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();
      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean deleteUser(final ProxyUser proxyUser)  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ServerResponse response = null;
          try {
            proxyUser.setPassword("dummy"); //password is required
            response = proxyService.deleteProxyUser(null, proxyUser.getProto(sessionId));
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();
      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean addGroup(final String groupName)  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ServerResponse response = null;
          try {
            response = proxyService.addGroup(null,
                SessionedStringProto.newBuilder()
                .setSessionId(sessionId)
                .setValue(groupName)
                .build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();
      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean deleteGroup(final String groupName)  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ServerResponse response = null;
          try {
            response = proxyService.deleteGroup(null,
                SessionedStringProto.newBuilder()
                    .setSessionId(sessionId)
                    .setValue(groupName)
                    .build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new IOException(response.getErrorMessage());
          }
        }
      }.withRetries();

      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public List<ProxyGroup> getUserGroups()  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<List<ProxyGroup>>(proxyServer) {
        public List<ProxyGroup> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ProxyGroupList groupList = null;
          try {
            groupList = proxyService.listProxyGroups(null, sessionId);
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (groupList.getResponse().getResultCode() == ResultCode.ERROR) {
            abort();
            throw new IOException(groupList.getResponse().getErrorMessage());
          }
          List<ProxyGroup> groups = new ArrayList<ProxyGroup>();
          for (ProxyGroupProto eachGroup: groupList.getGroupsList()) {
            groups.add(new ProxyGroup(eachGroup));
          }
          return groups;
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public List<ProxyUser> listProxyUsers()  throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      return new ProxyClientServerCallable<List<ProxyUser>>(proxyServer) {
        public List<ProxyUser> call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ProxyUserList userList = null;
          try {
            userList = proxyService.listProxyUsers(null, sessionId);
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (userList.getResponse().getResultCode() == ResultCode.ERROR) {
            abort();
            throw new IOException(userList.getResponse().getErrorMessage());
          }
          List<ProxyUser> users = new ArrayList<ProxyUser>();
          for (ProxyUserProto eachUser: userList.getUsersList()) {
            users.add(new ProxyUser(eachUser));
          }
          return users;
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean grantTables(final String groupName, final List<String> tableNames, final boolean grant) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          GrantTableRequest.Builder builder = GrantTableRequest.newBuilder();
          builder.setSessionId(sessionId);
          builder.setGrant(grant);
          builder.setGroupName(groupName);
          builder.addAllTableNames(tableNames);
          ServerResponse response = null;
          try {
            response = proxyService.grantTableToGroup(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ResultCode.ERROR) {
            abort();
            throw new IOException(response.getErrorMessage());
          }
          return true;
        }
      }.withRetries();
      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }


  public Boolean changeGroupUsers(final String groupName,
                                  final List<String> userIds,
                                  final boolean remove) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          GroupUserRequest.Builder builder = GroupUserRequest.newBuilder();
          builder.setSessionId(sessionId);
          builder.setGroupName(groupName);
          builder.addAllUserIds(userIds);
          builder.setRemove(remove);

          ServerResponse response = null;
          try {
            response = proxyService.changeGroupUsers(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ResultCode.ERROR) {
            abort();
            throw new IOException(response.getErrorMessage());
          }
          return true;
        }
      }.withRetries();
      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean changePassword(final String userId, final String password) throws IOException {
    try {
      final InetSocketAddress proxyServer = selectProxyServer();
      boolean success = new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws IOException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

          ChangePasswordRequest.Builder builder = ChangePasswordRequest.newBuilder();
          builder.setSessionId(sessionId);
          builder.setUserId(userId);
          try {
            builder.setPassword(ProxyUserAdmin.encodingPassword(password));
          } catch (Exception e) {
            abort();
            throw new IOException(e.getMessage(), e);
          }
          ServerResponse response = null;
          try {
            response = proxyService.changePassword(null, builder.build());
          } catch (ServiceException e) {
            abort();
            throw new IOException(e.getMessage(), e);
          } catch (Throwable t) {
            throw new IOException(t.getMessage(), t);
          }

          if (response.getResultCode() == ResultCode.ERROR) {
            abort();
            throw new IOException(response.getErrorMessage());
          }
          return true;
        }
      }.withRetries();

      if (success) {
        reloadUserDatas(proxyServer);
      }
      return success;
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public Boolean refreshSession(final InetSocketAddress proxyServer,
                              final SessionIdProto sessionId) throws IOException {
    try {
      return new ProxyClientServerCallable<Boolean>(proxyServer) {
        public Boolean call(NettyClientBase client) throws ServiceException {
          ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
          ServerResponse response = proxyService.refreshSession(null, sessionId);

          if (response.getResultCode() == ClientProtos.ResultCode.OK) {
            return response.getBoolResult().getValue();
          } else {
            abort();
            LOG.error(response.getErrorMessage());
            throw new ServiceException(response.getErrorMessage());
          }
        }
      }.withRetries();
    } catch (ServiceException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  private void reloadUserDatas(InetSocketAddress currentServer) throws IOException {
    for (InetSocketAddress eachServer: proxyServers) {
      if (eachServer.equals(currentServer)) {
        continue;
      }

      try {
        Boolean result = new ProxyClientServerCallable<Boolean>(eachServer) {
          public Boolean call(NettyClientBase client) throws IOException {
            ProxyServerClientProtocolService.BlockingInterface proxyService = client.getStub();
            SessionIdProto sessionId = checkSessionAndGet(client.getRemoteAddress(), proxyService);

            ServerResponse response = null;
            try {
              response = proxyService.reloadProxyUserMeta(null, sessionId);
            } catch (ServiceException e) {
              abort();
              throw new IOException(e.getMessage(), e);
            } catch (Throwable t) {
              throw new IOException(t.getMessage(), t);
            }

            if (response.getResultCode() == ResultCode.ERROR) {
              abort();
              throw new IOException(response.getErrorMessage());
            }
            return true;
          }
        }.withRetries();
      } catch (ServiceException e) {
        LOG.error(e.getMessage(), e);
      }
    }
  }

  abstract class ProxyClientServerCallable<T> extends ServerCallable<T> {
    public ProxyClientServerCallable(InetSocketAddress addr) {
      super(rpcPool, addr, ProxyServerClientProtocol.class, false);
    }
  }

  class SessionRefreshThread extends Thread {
    @Override
    public void run() {
      while (!closed.get()) {
        try {
          synchronized (this) {
            wait(30 * 60 * 1000);   //sleep 30 min
          }
        } catch (InterruptedException e) {
        }
        if (closed.get()) {
          break;
        }

        Map<InetSocketAddress, SessionIdProto> workingSessionIdMap = new HashMap<InetSocketAddress, SessionIdProto>();
        synchronized(sessionIdMap) {
          workingSessionIdMap.putAll(sessionIdMap);
        }

        for (Map.Entry<InetSocketAddress, SessionIdProto> entry: workingSessionIdMap.entrySet()) {
          try {
            refreshSession(entry.getKey(), entry.getValue());
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }
  }
}
