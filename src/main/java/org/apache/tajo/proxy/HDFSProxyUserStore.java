package org.apache.tajo.proxy;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.conf.TajoConf;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class HDFSProxyUserStore extends ProxyUserStore {
  Path dataFilePath;
  Path newDataFilePath;
  Path oldDataFilePath;
  Path lockFilePath;

  long lastDataFileTimestamp;

  public HDFSProxyUserStore() {
  }

  @Override
  public void init(TajoConf systemConf) throws Exception {
    super.init(systemConf);
    dataFilePath = new Path(systemConf.get("tajo.proxy.rootdir", "/tmp/tajo") + "/tajo-user.xml");
    newDataFilePath = new Path(dataFilePath.getParent(), "tajo-user.new.xml");
    oldDataFilePath = new Path(dataFilePath.getParent(), "tajo-user.old.xml");
    lockFilePath = new Path(dataFilePath.getParent(), "tajo-user.lock");
    try {
      FileSystem fs = dataFilePath.getFileSystem(systemConf);
      if (!fs.exists(dataFilePath.getParent())) {
        fs.mkdirs(dataFilePath.getParent());
      }

      if (!fs.exists(dataFilePath)) {
        fs.copyFromLocalFile(new Path("file://" + System.getProperty("user.dir") + "/conf/tajo-user.xml"),
            dataFilePath);
      }
      FileStatus file = fs.getFileStatus(dataFilePath);
      if (file != null) {
        lastDataFileTimestamp = file.getModificationTime();
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  @Override
  protected boolean isDataFileChanged() throws Exception {
    FileSystem fs = dataFilePath.getFileSystem(systemConf);
    FileStatus file = fs.getFileStatus(dataFilePath);
    if (file == null) {
      return false;
    }

    if (lastDataFileTimestamp != file.getModificationTime()) {
      lastDataFileTimestamp = file.getModificationTime();
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected InputStream openForRead() throws Exception {
    FileSystem fs = dataFilePath.getFileSystem(systemConf);
    FileStatus file = fs.getFileStatus(dataFilePath);
    if (file != null) {
      lastDataFileTimestamp = file.getModificationTime();
    }
    return  fs.open(dataFilePath);
  }

  @Override
  protected OutputStream openForWrite() throws Exception {
    FileSystem fs = newDataFilePath.getFileSystem(systemConf);
    return fs.create(newDataFilePath, true);
  }

  @Override
  protected boolean getLock() throws Exception {
    FileSystem fs = lockFilePath.getFileSystem(systemConf);
    OutputStream out = null;
    try {
      out = fs.create(lockFilePath, false);
      return true;
    } catch (Throwable e) {
      return false;
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }

  @Override
  protected boolean releaseLock() throws Exception {
    FileSystem fs = lockFilePath.getFileSystem(systemConf);
    if (fs.exists(oldDataFilePath)) {
      fs.delete(oldDataFilePath, false);
    }
    fs.rename(dataFilePath, oldDataFilePath);
    fs.rename(newDataFilePath, dataFilePath);

    FileStatus file = fs.getFileStatus(dataFilePath);
    if (file != null) {
      lastDataFileTimestamp = file.getModificationTime();
    }
    return fs.delete(lockFilePath, false);
  }
}
