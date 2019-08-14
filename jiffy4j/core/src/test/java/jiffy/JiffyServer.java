package jiffy;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import java.io.File;
import java.io.IOException;

class JiffyServer {

  protected Process handle;
  private String executable;

  JiffyServer(String executable) {
    this.executable = executable;
    handle = null;
  }

  public void start(String conf) throws IOException, InterruptedException {
    this.handle = startProcess(executable, "--config", conf);
  }

  public void stop() throws InterruptedException {
    if (this.handle != null) {
      handle.destroyForcibly();
      handle.waitFor();
    }
  }

  private Process startProcess(String... cmd) throws IOException {
    ProcessBuilder ps = new ProcessBuilder(cmd);
    File cmdFile = new File(cmd[0]);
    File log = new File("/tmp/" + cmdFile.getName() + ".log");
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

  public String getExecutable() {
    return executable;
  }

}
