package org.apache.tajo.proxy.cli;

import jline.console.history.FileHistory;

import java.io.File;
import java.io.IOException;

public class TajoFileHistory extends FileHistory {

  public TajoFileHistory(File file) throws IOException {
    super(file);
  }

  public void add(CharSequence item) {
    // skip add
  }

  public void addStatement(String item) {
    internalAdd(item);
  }
}
