package org.apache.tajo.jdbc.proxy;

import org.apache.tajo.QueryId;
import org.apache.tajo.QueryIdFactory;
import org.apache.tajo.ipc.ClientProtos;
import org.apache.tajo.jdbc.TajoResultSetBase;

import java.io.IOException;
import java.sql.*;

public class TajoStatement implements Statement {
  private TajoClientInterface tajoClient;
  private int fetchSize = TajoProxyClient.DEFAULT_FETCH_SIZE;
  private int maxRows = TajoProxyClient.DEFAULT_MAX_ROWS;
  /**
   * We need to keep a reference to the result set to support the following:
   * <code>
   * statement.execute(String sql);
   * statement.getResultSet();
   * </code>.
   */
  private TajoResultSetBase resultSet = null;

  /**
   * Add SQLWarnings to the warningChain if needed.
   */
  private SQLWarning warningChain = null;

  /**
   * Keep state so we can fail certain calls made after close().
   */
  private boolean isClosed = false;

  private QueryId lastQueryId;

  public TajoStatement(TajoClientInterface tajoClient) {
    this.tajoClient = tajoClient;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException("addBatch not supported");
  }

  @Override
  public void cancel() throws SQLException {
    if (lastQueryId != null) {
      try {
        tajoClient.killQuery(lastQueryId);
      } catch (IOException e) {
        throw new SQLException(e.getMessage(), e);
      }
    }
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("clearBatch not supported");
  }

  @Override
  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  @Override
  public void close() throws SQLException {
    if (resultSet != null) {
      resultSet.close();
    }
    lastQueryId = null;
    resultSet = null;
    isClosed = true;
  }

  public void closeOnCompletion() throws SQLException {
     // JDK 1.7
     throw new SQLFeatureNotSupportedException("closeOnCompletion not supported");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    resultSet = executeQuery(sql);

    return resultSet != null;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("execute not supported");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("execute not supported");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("execute not supported");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new SQLFeatureNotSupportedException("executeBatch not supported");
  }

  @Override
  public TajoResultSetBase executeQuery(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLFeatureNotSupportedException("Can't execute after statement has been closed");
    }

    try {
      resultSet = (TajoResultSetBase) executeQueryAndGetResult(sql);
      if(resultSet instanceof TajoResultSet) {
        ((TajoResultSet)resultSet).setMaxRows(maxRows);
      }
      return resultSet;
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  private ResultSet executeQueryAndGetResult(final String sql) throws IOException {
    try {
      ClientProtos.GetQueryStatusResponse response = tajoClient.executeQuery(sql);
      if (response.hasErrorMessage()) {
        throw new IOException(response.getErrorMessage());
      }
      this.lastQueryId = new QueryId(response.getQueryId());

      if (lastQueryId.equals(QueryIdFactory.NULL_QUERY_ID) && !response.hasHasResult()) {
        return tajoClient.createNullResultSet(lastQueryId);
      } else {
        return tajoClient.getQueryResultAndWait(lastQueryId, fetchSize);
      }
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    try {
      tajoClient.executeQuery(sql);

      return 1;
    } catch (Exception ex) {
      throw new SQLException(ex.toString());
    }
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException("executeUpdate not supported");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException("executeUpdate not supported");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException("executeUpdate not supported");
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new SQLFeatureNotSupportedException("getConnection not supported");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new SQLFeatureNotSupportedException("getFetchDirection not supported");
  }

  @Override
  public int getFetchSize() throws SQLException {
    return fetchSize;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLFeatureNotSupportedException("getGeneratedKeys not supported");
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMaxFieldSize not supported");
  }

  @Override
  public int getMaxRows() throws SQLException {
    return maxRows;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new SQLFeatureNotSupportedException("getMoreResults not supported");
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException("getMoreResults not supported");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new SQLFeatureNotSupportedException("getQueryTimeout not supported");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    return resultSet;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSetConcurrency not supported");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSetHoldability not supported");
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new SQLFeatureNotSupportedException("getResultSetType not supported");
  }

  @Override
  public int getUpdateCount() throws SQLException {
    return 0;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public boolean isCloseOnCompletion() throws SQLException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("isCloseOnCompletion not supported");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new SQLFeatureNotSupportedException("isPoolable not supported");
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException("setCursorName not supported");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException("setEscapeProcessing not supported");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException("setFetchDirection not supported");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    fetchSize = rows;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLFeatureNotSupportedException("setMaxFieldSize not supported");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    this.maxRows = max;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException("setPoolable not supported");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException("setQueryTimeout not supported");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("isWrapperFor not supported");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLFeatureNotSupportedException("unwrap not supported");
  }

}
