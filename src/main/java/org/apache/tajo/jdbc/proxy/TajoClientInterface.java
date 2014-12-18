package org.apache.tajo.jdbc.proxy;

import com.google.protobuf.ServiceException;
import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.proto.CatalogProtos.FunctionDescProto;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.ipc.ClientProtos.BriefQueryInfo;
import org.apache.tajo.ipc.ClientProtos.WorkerResourceInfo;
import org.apache.tajo.proxy.QueryHistory;
import org.apache.tajo.proxy.ipc.ProxyServerClientProtocol;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;

public interface TajoClientInterface {
  public void closeQuery(final QueryId queryId);
//  public ExplainQueryResponse explainQuery(final String sql) throws IOException;
  public ClientProtos.GetQueryStatusResponse executeQuery(final String sql) throws IOException;
  public ResultSet executeQueryAndGetResult(final String sql) throws ServiceException, IOException;
  public QueryStatus getQueryStatus(QueryId queryId) throws IOException;
  public ResultSet getQueryResult(QueryId queryId) throws IOException;
  public ResultSet getQueryResult(QueryId queryId, int fetchSize) throws IOException;
  public ProxyServerClientProtocol.ProxyQueryResult getProxyQueryResult(final QueryId queryId, int fetchSize) throws IOException;
  public ResultSet getQueryResultAndWait(QueryId queryId, int fetchSize) throws IOException;
  public boolean existTable(final String tableName) throws IOException;
  public boolean dropTable(final String tableName) throws IOException;
  public boolean dropTable(final String tableName, final boolean purge) throws IOException;
  public List<QueryHistory> listQueryHistory(String userId) throws IOException;
  public List<QueryHistory> listQueryHistory(String userId, boolean proxySubmitted) throws IOException;
  public QueryHistory getQueryHsitory(final String queryId) throws IOException;
  public List<WorkerResourceInfo> getClusterInfo() throws IOException;
  public List<String> getTableList(String databaeName) throws IOException;
  public TableDesc getTableDesc(final String tableName) throws IOException;
  public boolean killQuery(final QueryId queryId) throws IOException;
  public List<FunctionDescProto> getFunctions(final String functionName) throws IOException;
  public String getCurrentDatabase() throws IOException;
  public void close();
  public boolean isConnected();
  public boolean selectDatabase(final String databaseName) throws IOException;
  public List<String> getAllDatabaseNames() throws IOException;
  public boolean createDatabase(final String databaseName) throws IOException;
  public boolean dropDatabase(final String databaseName) throws IOException;
  public Boolean existDatabase(final String databaseName) throws IOException;
  public ResultSet createNullResultSet(QueryId queryId) throws Exception;
  public boolean login(String user, String password) throws IOException;
}
