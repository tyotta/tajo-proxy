package org.apache.tajo.proxy.cli;

public class InvalidStatementException extends Exception {
  public InvalidStatementException(String message) {
    super(message);
  }
}
