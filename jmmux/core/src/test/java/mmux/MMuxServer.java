package mmux;

import java.io.File;
import java.io.IOException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

class MMuxServer {

  Process handle;
  private String executable;

  MMuxServer(String executable) {
    this.executable = executable;
    handle = null;
  }

  void start(String conf) throws IOException, InterruptedException {
    this.handle = startProcess(executable, "--config", conf);
  }

  void stop() throws InterruptedException {
    if (this.handle != null) {
      handle.destroyForcibly();
      handle.waitFor();
    }
  }

  private Process startProcess(String... cmd) throws IOException {
    ProcessBuilder ps = new ProcessBuilder(cmd);
    File log = new File("/tmp/java_test.txt");
    ps.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
    ps.redirectErrorStream(true);
    return ps.start();
  }

  void waitTillServerReady(String host, int port) throws InterruptedException {
    boolean check = true;
    while (check) {
      try {
        TSocket sock = new TSocket(host, port);
        sock.open();
        sock.close();
        check = false;
      } catch (TTransportException e) {
        Thread.sleep(100);
      }
    }
  }

  String getExecutable() {
    return executable;
  }

}
