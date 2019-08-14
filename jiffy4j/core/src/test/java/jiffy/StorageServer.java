package jiffy;

import java.io.File;
import java.io.IOException;
import org.ini4j.Ini;

public class StorageServer extends JiffyServer {
  private String host;
  private int servicePort;
  private int managementPort;

  public StorageServer(String executable) {
    super(executable);
    host = null;
    servicePort = -1;
    managementPort = -1;
  }

  @Override
  public void start(String conf) throws IOException, InterruptedException {
    super.start(conf);
    Ini ini = new Ini();
    ini.load(new File(conf));
    host = ini.get("storage", "host");
    servicePort = Integer.parseInt(ini.get("storage", "service_port"));
    managementPort = Integer.parseInt(ini.get("storage", "management_port"));
    waitTillServerReady(host, servicePort);
    waitTillServerReady(host, managementPort);
  }

  @Override
  public void stop() throws InterruptedException {
    super.stop();
    host = null;
    servicePort = -1;
    managementPort = -1;
  }
}
