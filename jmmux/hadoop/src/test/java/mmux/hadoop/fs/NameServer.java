package mmux.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import mmux.DirectoryServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class NameServer extends DirectoryServer {
  public NameServer(String executable) {
    super(executable);
  }

  public MMuxFileSystem connectFS() {
    if (handle == null) {
      throw new RuntimeException("Cannot connect: server not running");
    }

    try {
      Configuration c = new Configuration();
      c.set(FileSystem.FS_DEFAULT_NAME_KEY, "mmfs://" + host + ":" + servicePort);
      MMuxFileSystem fs = new MMuxFileSystem();
      fs.initialize(URI.create("mmfs://" + host + ":" + servicePort), c);
      return fs;
    } catch (IOException e){
      System.out.println(e.getMessage());
      return new MMuxFileSystem();
    }
  }
}
