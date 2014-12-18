package org.apache.tajo.proxy.cli;

import org.apache.tajo.QueryId;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.cli.tsql.TajoCli;
import org.apache.tajo.client.QueryStatus;
import org.apache.tajo.proxy.cli.TajoProxyCli.TajoProxyCliContext;

import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.ResultSet;

public interface TajoProxyCliOutputFormatter {
  /**
   * Initialize formatter
   * @param context
   */
  public void init(TajoProxyCliContext context);

  /**
   * print query result to console
   * @param sout
   * @param sin
   * @param tableDesc
   * @param responseTime
   * @param res
   * @throws Exception
   */
  public void printResult(PrintWriter sout, InputStream sin, TableDesc tableDesc,
                          float responseTime, ResultSet res) throws Exception;

  /**
   * print no result message
   * @param sout
   */
  public void printNoResult(PrintWriter sout);

  /**
   * print simple message
   * @param sout
   * @param message
   */
  public void printMessage(PrintWriter sout, String message);

  /**
   * print query progress message
   * @param sout
   * @param status
   */
  public void printProgress(PrintWriter sout, QueryStatus status);

  /**
   * print error message
   * @param sout
   * @param t
   */
  public void printErrorMessage(PrintWriter sout, Throwable t);

  /**
   * print error message
   * @param sout
   * @param message
   */
  public void printErrorMessage(PrintWriter sout, String message);

  /**
   * print error message
   * @param sout
   * @param queryId
   */
  public void printKilledMessage(PrintWriter sout, QueryId queryId);

  /**
   * print query status error message
   * @param sout
   * @param status
   */
  void printErrorMessage(PrintWriter sout, QueryStatus status);

  void setScirptMode();
}
