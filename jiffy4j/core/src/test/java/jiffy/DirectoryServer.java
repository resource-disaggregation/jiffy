package jiffy;

import java.io.File;
import java.io.IOException;
import org.apache.thrift.TException;
import org.ini4j.Ini;

public class DirectoryServer extends JiffyServer {

  protected String host;
  protected int servicePort;
  private int leasePort;
  private int blockPort;

  public DirectoryServer(String executable) {
    super(executable);
    host = null;
    servicePort = -1;
    leasePort = -1;
    blockPort = -1;
  }

  @Override
  public void start(String conf) throws IOException, InterruptedException {
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
  public void stop() throws InterruptedException {
    super.stop();
    host = null;
    servicePort = -1;
    leasePort = -1;
    blockPort = -1;
  }

  public JiffyClient connect() throws TException {
    if (handle == null) {
      throw new RuntimeException("Cannot connect: server not running");
    }
    return new JiffyClient(host, servicePort, leasePort);
  }
}
