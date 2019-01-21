package jiffy.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import jiffy.DirectoryServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class NameServer extends DirectoryServer {
  public NameServer(String executable) {
    super(executable);
  }

  public JiffyFileSystem connectFS() {
    if (handle == null) {
      throw new RuntimeException("Cannot connect: server not running");
    }

    try {
      Configuration c = new Configuration();
      c.set(FileSystem.FS_DEFAULT_NAME_KEY, "jfs://" + host + ":" + servicePort);
      JiffyFileSystem fs = new JiffyFileSystem();
      fs.initialize(URI.create("jfs://" + host + ":" + servicePort), c);
      return fs;
    } catch (IOException e){
      System.out.println(e.getMessage());
      return new JiffyFileSystem();
    }
  }
}
