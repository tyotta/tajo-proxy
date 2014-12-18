package org.apache.tajo.proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.conf.TajoConf;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class ProxyUserStore {
  TajoConf systemConf;

  public ProxyUserStore() {
  }

  public void init(TajoConf systemConf) throws Exception {
    this.systemConf = systemConf;
  }
  protected abstract InputStream openForRead() throws Exception;
  protected abstract OutputStream openForWrite() throws Exception;
  protected abstract boolean getLock() throws Exception;
  protected abstract boolean releaseLock() throws Exception;
  protected abstract boolean isDataFileChanged() throws Exception;

  public synchronized void saveUsers(Configuration userConf) throws Exception {
    long startTime = System.currentTimeMillis();
    while (true) {
      if (getLock()) {
        break;
      }
      Thread.sleep(1000);
      if (System.currentTimeMillis() - startTime > 60 * 1000) {
        throw new IOException("Can't get lock for saving user management data.");
      }
    }

    OutputStream out = null;
    try {
      out = openForWrite();
      userConf.writeXml(out);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
        }
      }
      releaseLock();
    }
  }
}
