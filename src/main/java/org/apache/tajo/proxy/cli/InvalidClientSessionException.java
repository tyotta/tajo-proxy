package org.apache.tajo.proxy.cli;

import com.google.protobuf.ServiceException;

public class InvalidClientSessionException extends ServiceException {
  public InvalidClientSessionException(String message) {
    super(message);
  }
}
