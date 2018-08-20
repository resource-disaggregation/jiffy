package mmux;

import java.net.URI;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import mmux.hadoop.fs.MMuxFileSystem;
import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.IOException;
import org.apache.thrift.TException;
import org.ini4j.Ini;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;

class DirectoryServer extends MMuxServer {

  private String host;
  private int servicePort;
  private int leasePort;
  private int blockPort;

  DirectoryServer(String executable) {
    super(executable);
    host = null;
    servicePort = -1;
    leasePort = -1;
    blockPort = -1;
  }

  @Override
  void start(String conf) throws IOException, InterruptedException {
    super.start(conf);
    Ini ini = new Ini();
    ini.load(new File(conf));
    host = ini.get("directory", "host");
    servicePort = Integer.parseInt(ini.get("directory", "service_port"));
    leasePort = Integer.parseInt(ini.get("directory", "lease_port"));
    blockPort = Integer.parseInt(ini.get("directory", "block_port"));
    waitTillServerReady(host, servicePort);
    waitTillServerReady(host, leasePort);
    waitTillServerReady(host, blockPort);
  }

  @Override
  void stop() throws InterruptedException {
    super.stop();
    host = null;
    servicePort = -1;
    leasePort = -1;
    blockPort = -1;
  }

  MMuxFileSystem connect() throws TException {
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
