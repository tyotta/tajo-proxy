package org.apache.tajo.proxy;

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.conf.TajoConf;

import java.io.*;

public class LocalFileProxyUserStore extends ProxyUserStore {
  long lastDataFileTimestamp;

  public LocalFileProxyUserStore() {
  }

  public void init() throws Exception {
    super.init(systemConf);
    File file = new File(System.getProperty("user.dir") + "/conf/tajo-user.xml");
    if (file.exists()) {
      lastDataFileTimestamp = file.lastModified();
    }
  }

  @Override
  protected InputStream openForRead() throws Exception {
    return new FileInputStream(System.getProperty("user.dir") + "/conf/tajo-user.xml");
  }

  @Override
  protected OutputStream openForWrite() throws Exception {
    return new FileOutputStream(System.getProperty("user.dir") + "/conf/tajo-user.xml");
  }

  @Override
  protected boolean getLock() throws Exception {
    return true;
  }

  @Override
  protected boolean releaseLock() throws Exception {
    File file = new File(System.getProperty("user.dir") + "/conf/tajo-user.xml");
    if (file.exists()) {
      lastDataFileTimestamp = file.lastModified();
    }
    return true;
  }

  protected boolean isDataFileChanged() throws Exception {
    File file = new File(System.getProperty("user.dir") + "/conf/tajo-user.xml");
    if (file.exists()) {
      if (lastDataFileTimestamp != file.lastModified()) {
        lastDataFileTimestamp = file.lastModified();
        return true;
      }
    }
    return false;
  }
}
