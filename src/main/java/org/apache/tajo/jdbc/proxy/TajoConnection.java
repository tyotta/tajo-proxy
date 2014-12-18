package org.apache.tajo.jdbc.proxy;

import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.net.URI;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connection URL = jdbc:tajo-proxy//<proxyserver:port>/[database]?standby_proxy=<proxyserver_standby:port>[,</proxyserver_standby:port>][&characterEncoding=UTF-8]
 *
 * Connection Property: tajo-proxy.jdbc.rpc.workers
 *
 */
public class TajoConnection implements Connection {
  private TajoClientInterface tajoClient;

  private String databaseName;

  private AtomicBoolean closed = new AtomicBoolean(true);

  private String rawURI;

  private final Properties properties;

  private final URI uri;

  private final Map<String, List<String>> params;

  private String characterEncoding;

  public TajoConnection(String rawURI, Properties properties) throws SQLException {
    this.properties = properties;

    try {
      if (!rawURI.startsWith(TajoDriver.TAJO_PROXY_JDBC_URL_PREFIX)) {
        throw new SQLException("Invalid URL: " + rawURI, "TAJO-001");
      }

      // URI form: jdbc:tajo-proxy//hostname:port/databasename
      int startIdx = rawURI.indexOf(":");
      if (startIdx < 0) {
        throw new SQLException("Invalid URL: " + rawURI, "TAJO-001");
      }

      String uri = rawURI.substring(startIdx+1, rawURI.length());
      try {
        this.uri = URI.create(uri);
      } catch (IllegalArgumentException iae) {
        throw new SQLException("Invalid URL: " + rawURI, "TAJO-001");
      }

      String hostName = this.uri.getHost();
      int port = 0;
      if(hostName == null) {
        throw new SQLException("Invalid JDBC URI: " + rawURI, "TAJO-001");
      }
      if (this.uri.getPort() < 1) {
        throw new SQLException("Invalid URL(Wrong tajo-proxy master's host:port): " + rawURI, "TAJO-001");
      } else {
        port = this.uri.getPort();
      }

      if (this.uri.getPath() == null || this.uri.getPath().isEmpty()) { // if no database is given, set default.
        databaseName = TajoConstants.DEFAULT_DATABASE_NAME;
      } else {
        // getPath() will return '/database'.
        databaseName = this.uri.getPath().split("/")[1];
      }

      params = new QueryStringDecoder(rawURI).getParameters();

      TajoConf tajoConf = new TajoConf();

      if(properties != null) {
        for(Map.Entry<Object, Object> entry: properties.entrySet()) {
          tajoConf.set(entry.getKey().toString(), entry.getValue().toString());
        }
      }
      List<String> proxyServers = new ArrayList<String>();
      proxyServers.add(hostName + ":" + port);
      if (params.containsKey("standby_proxy")) {
        if (!params.get("standby_proxy").isEmpty()) {
          String[] standbyHosts = params.get("standby_proxy").get(0).split(",");

          for (String eachHost: standbyHosts) {
            String[] token = eachHost.split(":");
            if (token.length != 2) {
              throw new SQLException("Invalid URL(Wrong tajo-proxy standby's host:port): " + rawURI, "TAJO-001");
            }
            try {
              int standbyPort = Integer.parseInt(token[1]);
            } catch (NumberFormatException ne) {
              throw new SQLException("Invalid URL(Wrong tajo-proxy standby's host:port): " + rawURI, "TAJO-001");
            }

            proxyServers.add(eachHost);
          }
        }
      }

      if (params.containsKey("characterEncoding")) {
        characterEncoding = params.get("characterEncoding").get(0);
      }
      tajoClient = new TajoProxyClient(tajoConf, proxyServers, databaseName, characterEncoding);

      String user = (String)properties.get("user");
      String password = (String)properties.get("password");

      tajoClient.login(user, password);
    } catch (SQLException se) {
      if (tajoClient != null) {
        tajoClient.close();
      }
      throw se;

    } catch (Throwable t) { // for unexpected exceptions like ArrayIndexOutOfBoundsException.
      t.printStackTrace();
      if (tajoClient != null) {
        tajoClient.close();
      }
      throw new SQLException("Connection error: " + rawURI, "TAJO-001");
    } finally {

    }
    closed.set(false);
  }

  public String getUri() {
    return rawURI;
  }

  public TajoClientInterface getTajoClient() {
    return tajoClient;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getCharacterEncoding() {
    return characterEncoding;
  }

  @Override
  public void clearWarnings() throws SQLException {
  }

  @Override
  public void close() throws SQLException {
    if(!closed.get()) {
      if(tajoClient != null) {
        tajoClient.close();
      }

      closed.set(true);
    }
  }

  @Override
  public void commit() throws SQLException {
    throw new SQLFeatureNotSupportedException("commit");
  }

  @Override
  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    throw new SQLFeatureNotSupportedException("createArrayOf");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createBlob");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createClob");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new SQLFeatureNotSupportedException("createNClob");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new SQLFeatureNotSupportedException("createSQLXML");
  }

  @Override
  public Statement createStatement() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    return new TajoStatement(tajoClient);
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("createStatement");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                   int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("createStatement");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("createStruct");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  @Override
  public String getCatalog() throws SQLException {
    try {
      return tajoClient.getCurrentDatabase();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new SQLFeatureNotSupportedException("getClientInfo");
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("getClientInfo");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("getHoldability");
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new TajoDatabaseMetaData(this);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLFeatureNotSupportedException("getTypeMap");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new SQLFeatureNotSupportedException("getWarnings");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return closed.get();
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return tajoClient.isConnected();
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("nativeSQL");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareCall");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareCall");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
                                       int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareCall");
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new TajoPreparedStatement(tajoClient, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return new TajoPreparedStatement(tajoClient, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareStatement");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareStatement");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency) throws SQLException {
    return new TajoPreparedStatement(tajoClient, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
                                            int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLFeatureNotSupportedException("prepareStatement");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("releaseSavepoint");
  }

  @Override
  public void rollback() throws SQLException {
    throw new SQLFeatureNotSupportedException("rollback");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException("rollback");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new SQLFeatureNotSupportedException("setAutoCommit");
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    try {
      tajoClient.selectDatabase(catalog);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    throw new UnsupportedOperationException("setClientInfo");
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    throw new UnsupportedOperationException("setClientInfo");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new SQLFeatureNotSupportedException("setHoldability");
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new SQLFeatureNotSupportedException("setReadOnly");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException("setSavepoint");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("setSavepoint");
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTransactionIsolation");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException("setTypeMap");
  }

  @Override
  public <T> T unwrap(Class<T> tClass) throws SQLException {
    if (isWrapperFor(tClass)) {
      return (T) this;
    }
    throw new SQLException("No wrapper for " + tClass);
  }

  @Override
  public boolean isWrapperFor(Class<?> tClass) throws SQLException {
    return tClass.isInstance(this);
  }

  public void abort(Executor executor) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("abort not supported");
  }

  public int getNetworkTimeout() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("getNetworkTimeout not supported");
  }

  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("setNetworkTimeout not supported");
  }

  public String getSchema() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("getSchema not supported");
  }

  public void setSchema(String schema) throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("setSchema not supported");
  }
}
