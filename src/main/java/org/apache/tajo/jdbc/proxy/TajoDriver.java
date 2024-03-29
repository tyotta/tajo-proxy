package org.apache.tajo.jdbc.proxy;

import org.apache.tajo.conf.TajoConf;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Tajo JDBC Driver via TajoProxyServer <p/>
 *
 */
public class TajoDriver implements Driver, Closeable {
  public static final int MAJOR_VERSION = 1;
  public static final int MINOR_VERSION = 0;

  public static final int JDBC_VERSION_MAJOR = 4;
  public static final int JDBC_VERSION_MINOR = 0;

  public static final String TAJO_PROXY_JDBC_URL_PREFIX = "jdbc:tajo-proxy://";

  protected static TajoConf jdbcTajoConf = new TajoConf();

  static {
    try {
      DriverManager.registerDriver(new TajoDriver());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public TajoDriver() {
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public Connection connect(String url, Properties properties) throws SQLException {
    try {
      return new TajoConnection(url, properties);
    } catch (Exception e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(TAJO_PROXY_JDBC_URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  @Override
  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("getParentLogger not supported");
  }
}
