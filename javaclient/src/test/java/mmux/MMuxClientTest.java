package mmux;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import junit.framework.TestCase;
import mmux.directory.rpc_storage_mode;
import mmux.kv.KVClient;
import mmux.kv.KVClient.RedirectException;
import mmux.kv.KVClient.RedoException;
import mmux.notification.KVListener;
import mmux.notification.event.Notification;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;

public class MMuxClientTest extends TestCase {

  private String directoryConfFile;
  private String storage1ConfFile;
  private String storage2ConfFile;
  private String storage3ConfFile;
  private String storageASConfFile;

  private Ini directoryConf;
  private Ini storage1Conf;
  private Ini storage2Conf;
  private Ini storage3Conf;
  private Ini storageASConf;

  private Process directoryd;
  private Process storaged1;
  private Process storaged2;
  private Process storaged3;

  private String dirHost;
  private int dirPort;
  private int leasePort;

  public void setUp() throws Exception {
    super.setUp();
    directoryConfFile = this.getClass().getResource("/directory.conf").getFile();
    storage1ConfFile = this.getClass().getResource("/storage1.conf").getFile();
    storage2ConfFile = this.getClass().getResource("/storage2.conf").getFile();
    storage3ConfFile = this.getClass().getResource("/storage3.conf").getFile();
    storageASConfFile = this.getClass().getResource("/storage_auto_scale.conf").getFile();

    directoryConf = new Ini(new File(directoryConfFile));
    storage1Conf = new Ini(new File(storage1ConfFile));
    storage2Conf = new Ini(new File(storage2ConfFile));
    storage3Conf = new Ini(new File(storage3ConfFile));
    storageASConf = new Ini(new File(storageASConfFile));

    dirHost = directoryConf.get("directory", "host");
    dirPort = Integer.parseInt(directoryConf.get("directory", "service_port"));
    leasePort = Integer.parseInt(directoryConf.get("directory", "lease_port"));
  }

  private Process startProcess(String... cmd) throws IOException {
    ProcessBuilder ps = new ProcessBuilder(cmd);
    File log = new File("/tmp/java_test.txt");
    ps.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
    ps.redirectErrorStream(true);
    return ps.start();
  }

  private void stopProcess(Process process) throws InterruptedException {
    process.destroyForcibly();
    process.waitFor();
    if (process.isAlive()) {
      System.err.println("WARN: Process is still alive");
    }
  }

  private void waitTillServerReady(String host, int port) throws InterruptedException {
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

  private void startServers(boolean chain, boolean autoScale) throws InterruptedException {

    String dirExec = System.getProperty("mmux.directory.exec", "directoryd");
    String storageExec = System.getProperty("mmux.storage.exec", "storaged");

    try {
      directoryd = startProcess(dirExec, "--config", directoryConfFile);
    } catch (IOException e) {
      throw new InterruptedException(
          String.format("Error running executable %s: %s\n", dirExec, e.getMessage()));
    }
    Section dconf = directoryConf.get("directory");
    waitTillServerReady(dconf.get("host"), Integer.parseInt(dconf.get("service_port")));
    waitTillServerReady(dconf.get("host"), Integer.parseInt(dconf.get("lease_port")));
    waitTillServerReady(dconf.get("host"), Integer.parseInt(dconf.get("block_port")));

    String conf1 = autoScale ? storageASConfFile : storage1ConfFile;
    try {
      storaged1 = startProcess(storageExec, "--config", conf1);
    } catch (IOException e) {
      throw new InterruptedException(
          String.format("Error running executable %s: %s\n", storageExec, e.getMessage()));
    }
    Section s1conf = (autoScale ? storageASConf : storage1Conf).get("storage");
    waitTillServerReady(s1conf.get("host"), Integer.parseInt(s1conf.get("service_port")));
    waitTillServerReady(s1conf.get("host"), Integer.parseInt(s1conf.get("management_port")));
    waitTillServerReady(s1conf.get("host"), Integer.parseInt(s1conf.get("notification_port")));
    waitTillServerReady(s1conf.get("host"), Integer.parseInt(s1conf.get("chain_port")));

    if (chain) {
      try {
        storaged2 = startProcess(storageExec, "--config", storage2ConfFile);
      } catch (IOException e) {
        throw new InterruptedException(
            String.format("Error running executable %s: %s\n", storageExec, e.getMessage()));
      }
      Section s2conf = storage2Conf.get("storage");
      waitTillServerReady(s2conf.get("host"), Integer.parseInt(s2conf.get("service_port")));
      waitTillServerReady(s2conf.get("host"), Integer.parseInt(s2conf.get("management_port")));
      waitTillServerReady(s2conf.get("host"), Integer.parseInt(s2conf.get("notification_port")));
      waitTillServerReady(s2conf.get("host"), Integer.parseInt(s2conf.get("chain_port")));

      try {
        storaged3 = startProcess(storageExec, "--config", storage3ConfFile);
      } catch (IOException e) {
        throw new InterruptedException(
            String.format("Error running executable %s: %s\n", storageExec, e.getMessage()));
      }
      Section s3conf = storage3Conf.get("storage");
      waitTillServerReady(s3conf.get("host"), Integer.parseInt(s3conf.get("service_port")));
      waitTillServerReady(s3conf.get("host"), Integer.parseInt(s3conf.get("management_port")));
      waitTillServerReady(s3conf.get("host"), Integer.parseInt(s3conf.get("notification_port")));
      waitTillServerReady(s3conf.get("host"), Integer.parseInt(s3conf.get("chain_port")));
    } else {
      storaged2 = null;
      storaged3 = null;
    }
  }

  private void stopServers() throws InterruptedException {
    stopProcess(directoryd);
    stopProcess(storaged1);
    if (storaged2 != null) {
      stopProcess(storaged2);
    } else if (storaged3 != null) {
      stopProcess(storaged3);
    }
  }

  private void kvOps(KVClient kv) throws TException, RedoException, RedirectException {
    for (int i = 0; i < 1000; i++) {
      assertEquals("ok", kv.put(String.valueOf(i), String.valueOf(i)));
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(String.valueOf(i), kv.get(String.valueOf(i)));
    }

    for (int i = 1000; i < 2000; i++) {
      assertEquals(null, kv.get(String.valueOf(i)));
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(String.valueOf(i), kv.update(String.valueOf(i), String.valueOf(i + 1000)));
    }

    for (int i = 1000; i < 2000; i++) {
      assertEquals(null, kv.update(String.valueOf(i), String.valueOf(i + 1000)));
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(String.valueOf(i + 1000), kv.get(String.valueOf(i)));
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(String.valueOf(i + 1000), kv.remove(String.valueOf(i)));
    }

    for (int i = 1000; i < 2000; i++) {
      assertEquals(null, kv.remove(String.valueOf(i)));
    }

    for (int i = 0; i < 1000; i++) {
      assertEquals(null, kv.remove(String.valueOf(i)));
    }
  }

  public void testLeaseWorker() throws InterruptedException, TException, IOException {
    System.err.println("Testing Lease Worker");
    startServers(false, false);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      client.create("/a/file.txt", "local://tmp", 1, 1);
      assertTrue(client.fs().exists("/a/file.txt"));
      Thread.sleep(client.getWorker().getRenewalDurationMs());
      assertTrue(client.fs().exists("/a/file.txt"));
      Thread.sleep(client.getWorker().getRenewalDurationMs());
      assertTrue(client.fs().exists("/a/file.txt"));
    } finally {
      stopServers();
    }
  }

  public void testCreate()
      throws InterruptedException, TException, IOException, RedoException, RedirectException {
    System.err.println("Testing Create");
    startServers(false, false);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      KVClient kv = client.create("/a/file.txt", "local://tmp", 1, 1);
      kvOps(kv);
      assertTrue(client.fs().exists("/a/file.txt"));
    } finally {
      stopServers();
    }
  }

  public void testFlushRemove() throws InterruptedException, TException, IOException {
    System.err.println("Testing Flush, Remove");
    startServers(false, false);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      client.create("/a/file.txt", "local://tmp", 1, 1);
      assertTrue(client.getWorker().hasPath("/a/file.txt"));
      client.flush("/a/file.txt", "local://tmp");
      assertFalse(client.getWorker().hasPath("/a/file.txt"));
      assertEquals(rpc_storage_mode.rpc_on_disk, client.fs().dstatus("/a/file.txt").storage_mode);
      client.remove("/a/file.txt");
      assertFalse(client.getWorker().hasPath("/a/file.txt"));
      assertFalse(client.fs().exists("/a/file.txt"));
    } finally {
      stopServers();
    }
  }

  public void testOpen()
      throws InterruptedException, TException, IOException, RedoException, RedirectException {
    System.err.println("Testing Open");
    startServers(false, false);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      client.create("/a/file.txt", "local://tmp", 1, 1);
      assertTrue(client.fs().exists("/a/file.txt"));
      KVClient kv = client.open("/a/file.txt");
      kvOps(kv);
    } finally {
      stopServers();
    }
  }

  public void testChainReplication()
      throws InterruptedException, TException, IOException, RedoException, RedirectException {
    System.err.println("Testing Chain Replication");
    startServers(true, false);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      KVClient kv = client.create("/a/file.txt", "local://tmp", 1, 3);
      assertEquals(3, client.fs().dstatus("/a/file.txt").chain_length);
      kvOps(kv);
    } finally {
      stopServers();
    }
  }

  public void testAutoScale()
      throws InterruptedException, TException, IOException, RedoException, RedirectException {
    System.err.println("Testing Auto-Scale");
    startServers(false, true);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      KVClient kv = client.create("/a/file.txt", "local://tmp", 1, 1);
      for (int i = 0; i < 2000; i++) {
        assertEquals("ok", kv.put(String.valueOf(i), String.valueOf(i)));
      }
      assertEquals(4, client.fs().dstatus("/a/file.txt").data_blocks.size());
      for (int i = 0; i < 2000; i++) {
        assertEquals(String.valueOf(i), kv.remove(String.valueOf(i)));
      }
      assertEquals(1, client.fs().dstatus("/a/file.txt").data_blocks.size());
    } finally {
      stopServers();
    }
  }

  public void testNotifications()
      throws InterruptedException, IOException, TException, RedoException, RedirectException {
    System.err.println("Testing Notifications");
    startServers(false, false);
    try (MMuxClient client = new MMuxClient(dirHost, dirPort, leasePort)) {
      String op1 = "put", op2 = "remove";
      String key = "key1", value = "value1";

      client.create("/a/file.txt", "local://tmp", 1, 1);
      KVListener n1 = client.listen("/a/file.txt");
      KVListener n2 = client.listen("/a/file.txt");
      KVListener n3 = client.listen("/a/file.txt");

      n1.subscribe(Collections.singletonList(op1));
      n2.subscribe(Arrays.asList(op1, op2));
      n3.subscribe(Collections.singletonList(op2));

      KVClient kv = client.open("/a/file.txt");
      kv.put(key, value);
      kv.remove(key);

      Notification N1 = n1.getNotification();
      assertEquals(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), N1.getData());
      assertEquals(op1, N1.kind().name());

      Notification N2 = n2.getNotification();
      assertEquals(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), N2.getData());
      assertEquals(op1, N2.kind().name());

      Notification N3 = n2.getNotification();
      assertEquals(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), N3.getData());
      assertEquals(op2, N3.kind().name());

      Notification N4 = n3.getNotification();
      assertEquals(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), N4.getData());
      assertEquals(op2, N4.kind().name());

      n1.unsubscribe(Collections.singletonList(op1));
      n2.unsubscribe(Collections.singletonList(op2));

      kv.put(key, value);
      kv.remove(key);

      Notification N5 = n2.getNotification();
      assertEquals(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), N5.getData());
      assertEquals(op1, N5.kind().name());

      Notification N6 = n3.getNotification();
      assertEquals(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)), N6.getData());
      assertEquals(op2, N6.kind().name());
    } finally {
      stopServers();
    }
  }
}
