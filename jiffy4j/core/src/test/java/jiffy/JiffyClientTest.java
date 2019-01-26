package jiffy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import jiffy.directory.Flags;
import jiffy.directory.Permissions;
import jiffy.storage.HashTableClient;
import jiffy.storage.HashTableClient.LockedClient;
import jiffy.notification.KVListener;
import jiffy.notification.event.Notification;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class JiffyClientTest {

  @Rule
  public TestName testName = new TestName();

  private DirectoryServer directoryServer;
  private StorageServer storageServer1;
  private StorageServer storageServer2;
  private StorageServer storageServer3;

  public JiffyClientTest() {
    directoryServer = new DirectoryServer(System.getProperty("jiffy.directory.exec", "directoryd"));
    storageServer1 = new StorageServer(System.getProperty("jiffy.storage.exec", "storaged"));
    storageServer2 = new StorageServer(System.getProperty("jiffy.storage.exec", "storaged"));
    storageServer3 = new StorageServer(System.getProperty("jiffy.storage.exec", "storaged"));
  }

  @Before
  public void setUp() {
    System.out.println("=> Running " + testName.getMethodName());
  }

  @After
  public void tearDown() throws InterruptedException {
    stopServers();
  }

  private void stopServers() throws InterruptedException {
    storageServer1.stop();
    storageServer2.stop();
    storageServer3.stop();
    directoryServer.stop();
  }

  private void startServers(boolean chain, boolean autoScale) throws InterruptedException {

    try {
      directoryServer.start(this.getClass().getResource("/directory.conf").getFile());
    } catch (IOException e) {
      throw new InterruptedException(
          String.format("Error running executable %s: %s\n", directoryServer.getExecutable(),
              e.getMessage()));
    }

    String conf1 = autoScale ? this.getClass().getResource("/storage_auto_scale.conf").getFile()
        : this.getClass().getResource("/storage1.conf").getFile();
    try {
      storageServer1.start(conf1);
    } catch (IOException e) {
      throw new InterruptedException(
          String.format("Error running executable %s: %s\n", storageServer1.getExecutable(),
              e.getMessage()));
    }

    if (chain) {
      try {
        storageServer2.start(this.getClass().getResource("/storage2.conf").getFile());
      } catch (IOException e) {
        throw new InterruptedException(
            String.format("Error running executable %s: %s\n", storageServer2.getExecutable(),
                e.getMessage()));
      }

      try {
        storageServer3.start(this.getClass().getResource("/storage3.conf").getFile());
      } catch (IOException e) {
        throw new InterruptedException(
            String.format("Error running executable %s: %s\n", storageServer3.getExecutable(),
                e.getMessage()));
      }
    }
  }

  private ByteBuffer makeBB(int i) {
    return ByteBufferUtils.fromString(String.valueOf(i));
  }

  private ByteBuffer makeBB(String str) {
    return ByteBufferUtils.fromString(str);
  }

  private void kvOps(HashTableClient kv) throws TException {
    System.out.println("==> Testing KV ops");
    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB("!ok"), kv.put(makeBB(i), makeBB(i)));
    }

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB(i), kv.get(makeBB(i)));
    }

    for (int i = 1000; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.get(makeBB(i)));
    }

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB(i), kv.update(makeBB(i), makeBB(i + 1000)));
    }

    for (int i = 1000; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.update(makeBB(i), makeBB(i + 1000)));
    }

    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals(makeBB("!ok"), kv.upsert(makeBB(i), makeBB(i + 1000)));
    }

    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals(makeBB(i + 1000), kv.get(makeBB(i)));
    }

    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals(makeBB(i + 1000), kv.remove(makeBB(i)));
    }

    for (int i = 0; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.get(makeBB(i)));
    }

    for (int i = 1000; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.remove(makeBB(i)));
    }

    List<ByteBuffer> validKeys = new ArrayList<>(1000);
    List<ByteBuffer> invalidKeys = new ArrayList<>(1000);
    List<ByteBuffer> originalValues = new ArrayList<>(1000);
    List<ByteBuffer> updatedValues = new ArrayList<>(1000);
    List<ByteBuffer> originalKVs = new ArrayList<>(2000);
    List<ByteBuffer> updatedKVs = new ArrayList<>(2000);
    List<ByteBuffer> invalidKVs = new ArrayList<>(2000);
    initBatchOps(validKeys, invalidKeys, originalValues, updatedValues, originalKVs, updatedKVs,
        invalidKVs);
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!ok")), kv.put(originalKVs));
    Assert.assertEquals(originalValues, kv.get(validKeys));
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.get(invalidKeys));
    Assert.assertEquals(originalValues, kv.update(updatedKVs));
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.update(invalidKVs));
    Assert.assertEquals(updatedValues, kv.get(validKeys));
    Assert.assertEquals(updatedValues, kv.remove(validKeys));
    Assert
        .assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.remove(invalidKeys));
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.get(validKeys));

    LockedClient lkv = kv.lock();
    lockedKVOps(lkv);
    lkv.close();
  }

  private void lockedKVOps(HashTableClient.LockedClient kv) throws TException {
    System.out.println("==> Testing locked KV ops");
    Assert.assertEquals(0, kv.numKeys());
    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB("!ok"), kv.put(makeBB(i), makeBB(i)));
    }
    Assert.assertEquals(1000, kv.numKeys());

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB(i), kv.get(makeBB(i)));
    }

    for (int i = 1000; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.get(makeBB(i)));
    }

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB(i), kv.update(makeBB(i), makeBB(i + 1000)));
    }

    Assert.assertEquals(1000, kv.numKeys());

    for (int i = 1000; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.update(makeBB(i), makeBB(i + 1000)));
    }

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB(i + 1000), kv.get(makeBB(i)));
    }

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB(i + 1000), kv.remove(makeBB(i)));
    }

    Assert.assertEquals(0, kv.numKeys());

    for (int i = 1000; i < 2000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.remove(makeBB(i)));
    }

    for (int i = 0; i < 1000; i++) {
      Assert.assertEquals(makeBB("!key_not_found"), kv.get(makeBB(i)));
    }

    List<ByteBuffer> validKeys = new ArrayList<>(1000);
    List<ByteBuffer> invalidKeys = new ArrayList<>(1000);
    List<ByteBuffer> originalValues = new ArrayList<>(1000);
    List<ByteBuffer> updatedValues = new ArrayList<>(1000);
    List<ByteBuffer> originalKVs = new ArrayList<>(2000);
    List<ByteBuffer> updatedKVs = new ArrayList<>(2000);
    List<ByteBuffer> invalidKVs = new ArrayList<>(2000);
    initBatchOps(validKeys, invalidKeys, originalValues, updatedValues, originalKVs, updatedKVs,
        invalidKVs);

    Assert.assertEquals(Collections.nCopies(1000, makeBB("!ok")), kv.put(originalKVs));
    Assert.assertEquals(1000, kv.numKeys());
    Assert.assertEquals(originalValues, kv.get(validKeys));
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.get(invalidKeys));
    Assert.assertEquals(originalValues, kv.update(updatedKVs));
    Assert.assertEquals(1000, kv.numKeys());
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.update(invalidKVs));
    Assert.assertEquals(updatedValues, kv.get(validKeys));
    Assert.assertEquals(updatedValues, kv.remove(validKeys));
    Assert.assertEquals(0, kv.numKeys());
    Assert
        .assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.remove(invalidKeys));
    Assert.assertEquals(Collections.nCopies(1000, makeBB("!key_not_found")), kv.get(validKeys));
  }

  private void initBatchOps(List<ByteBuffer> validKeys, List<ByteBuffer> invalidKeys,
      List<ByteBuffer> originalValues, List<ByteBuffer> updatedValues, List<ByteBuffer> originalKVs,
      List<ByteBuffer> updatedKVs, List<ByteBuffer> invalidKVs) {
    for (int i = 0; i < 1000; i++) {
      validKeys.add(makeBB(i));
      originalValues.add(makeBB(i));
      originalKVs.add(makeBB(i));
      originalKVs.add(makeBB(i));
      updatedKVs.add(makeBB(i));
      updatedKVs.add(makeBB(i + 1000));
      invalidKeys.add(makeBB(i + 1000));
      updatedValues.add(makeBB(i + 1000));
      invalidKVs.add(makeBB(i + 1000));
      invalidKVs.add(makeBB(i + 2000));
    }
  }

  @Test
  public void testLeaseWorker() throws InterruptedException, TException, IOException {
    startServers(false, false);
    try (JiffyClient client = directoryServer.connect()) {
      client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
      Assert.assertTrue(client.fs().exists("/a/file.txt"));
      Thread.sleep(client.getWorker().getRenewalDurationMs());
      Assert.assertTrue(client.fs().exists("/a/file.txt"));
      Thread.sleep(client.getWorker().getRenewalDurationMs());
      Assert.assertTrue(client.fs().exists("/a/file.txt"));
    } finally {
      stopServers();
    }
  }

  @Test
  public void testCreate() throws InterruptedException, TException, IOException {
    startServers(false, false);
    try (JiffyClient client = directoryServer.connect()) {
      HashTableClient kv = client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
      kvOps(kv);
      Assert.assertTrue(client.fs().exists("/a/file.txt"));
    } finally {
      stopServers();
    }
  }

  @Test
  public void testOpen() throws InterruptedException, TException, IOException {
    startServers(false, false);
    try (JiffyClient client = directoryServer.connect()) {
      client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
      Assert.assertTrue(client.fs().exists("/a/file.txt"));
      HashTableClient kv = client.open("/a/file.txt");
      kvOps(kv);
    } finally {
      stopServers();
    }
  }

  @Test
  public void testFlushRemove() throws InterruptedException, TException, IOException {
    startServers(false, false);
    try (JiffyClient client = directoryServer.connect()) {
      client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
      Assert.assertTrue(client.getWorker().hasPath("/a/file.txt"));
      client.sync("/a/file.txt", "local://tmp");
      Assert.assertTrue(client.getWorker().hasPath("/a/file.txt"));
      client.remove("/a/file.txt");
      Assert.assertFalse(client.getWorker().hasPath("/a/file.txt"));
      Assert.assertFalse(client.fs().exists("/a/file.txt"));
    } finally {
      stopServers();
    }
  }

  @Test
  public void testClose() throws InterruptedException, TException, IOException {
    startServers(false, false);
    try (JiffyClient client = directoryServer.connect()) {
      client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
      client.createHashTable("/a/file1.txt", "local://tmp", 1, 1, Flags.PINNED, Permissions.all,
          Collections.emptyMap());
      client.createHashTable("/a/file2.txt", "local://tmp", 1, 1, Flags.MAPPED, Permissions.all,
          Collections.emptyMap());
      Assert.assertTrue(client.getWorker().hasPath("/a/file.txt"));
      Assert.assertTrue(client.getWorker().hasPath("/a/file1.txt"));
      Assert.assertTrue(client.getWorker().hasPath("/a/file2.txt"));
      client.close("/a/file.txt");
      client.close("/a/file1.txt");
      client.close("/a/file2.txt");
      Assert.assertFalse(client.getWorker().hasPath("/a/file.txt"));
      Assert.assertFalse(client.getWorker().hasPath("/a/file1.txt"));
      Assert.assertFalse(client.getWorker().hasPath("/a/file2.txt"));
      Thread.sleep(client.getWorker().getRenewalDurationMs());
      Assert.assertTrue(client.fs().exists("/a/file.txt"));
      Assert.assertTrue(client.fs().exists("/a/file1.txt"));
      Assert.assertTrue(client.fs().exists("/a/file2.txt"));
      Thread.sleep(client.getWorker().getRenewalDurationMs() * 2);
      Assert.assertFalse(client.fs().exists("/a/file.txt"));
      Assert.assertTrue(client.fs().exists("/a/file1.txt"));
      Assert.assertTrue(client.fs().exists("/a/file2.txt"));
    } finally {
      stopServers();
    }
  }

  @Test
  public void testChainReplication() throws InterruptedException, TException, IOException {
    startServers(true, false);
    try (JiffyClient client = directoryServer.connect()) {
      HashTableClient kv = client.createHashTable("/a/file.txt", "local://tmp", 1, 3);
      Assert.assertEquals(3, client.fs().dstatus("/a/file.txt").chain_length);
      kvOps(kv);
    } finally {
      stopServers();
    }
  }

  @Test
  public void testFailures() throws InterruptedException, TException, IOException {
    List<StorageServer> servers = Arrays.asList(storageServer1, storageServer2, storageServer3);
    for (StorageServer server : servers) {
      startServers(true, false);
      try (JiffyClient client = directoryServer.connect()) {
        HashTableClient kv = client.createHashTable("/a/file.txt", "local://tmp", 1, 3);
        Assert.assertEquals(3, client.fs().dstatus("/a/file.txt").chain_length);
        server.stop();
        kvOps(kv);
      } finally {
        stopServers();
      }
    }
  }

//  @Test
//  public void testAutoScale() throws InterruptedException, TException, IOException {
//    startServers(false, true);
//    try (JiffyClient client = directoryServer.connect()) {
//      HashTableClient kv = client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
//      for (int i = 0; i < 2000; i++) {
//        Assert.assertEquals(makeBB("!ok"), kv.put(makeBB(i), makeBB(i)));
//      }
//      Assert.assertEquals(4, client.fs().dstatus("/a/file.txt").data_blocks.size());
//      for (int i = 0; i < 2000; i++) {
//        Assert.assertEquals(makeBB(i), kv.remove(makeBB(i)));
//      }
//      Assert.assertEquals(1, client.fs().dstatus("/a/file.txt").data_blocks.size());
//    } finally {
//      stopServers();
//    }
//  }

  @Test
  public void testNotifications() throws InterruptedException, IOException, TException {
    startServers(false, false);
    try (JiffyClient client = directoryServer.connect()) {
      String op1 = "put", op2 = "remove";
      ByteBuffer key = ByteBufferUtils.fromString("key1");
      ByteBuffer value = ByteBufferUtils.fromString("value1");

      client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
      KVListener n1 = client.listen("/a/file.txt");
      KVListener n2 = client.listen("/a/file.txt");
      KVListener n3 = client.listen("/a/file.txt");

      n1.subscribe(Collections.singletonList(op1));
      n2.subscribe(Arrays.asList(op1, op2));
      n3.subscribe(Collections.singletonList(op2));

      HashTableClient kv = client.open("/a/file.txt");
      kv.put(key, value);
      kv.remove(key);

      Notification N1 = n1.getNotification();
      Assert.assertEquals(key, N1.getData());
      Assert.assertEquals(op1, N1.kind().name());

      Notification N2 = n2.getNotification();
      Assert.assertEquals(key, N2.getData());
      Assert.assertEquals(op1, N2.kind().name());

      Notification N3 = n2.getNotification();
      Assert.assertEquals(key, N3.getData());
      Assert.assertEquals(op2, N3.kind().name());

      Notification N4 = n3.getNotification();
      Assert.assertEquals(key, N4.getData());
      Assert.assertEquals(op2, N4.kind().name());

      n1.unsubscribe(Collections.singletonList(op1));
      n2.unsubscribe(Collections.singletonList(op2));

      kv.put(key, value);
      kv.remove(key);

      Notification N5 = n2.getNotification();
      Assert.assertEquals(key, N5.getData());
      Assert.assertEquals(op1, N5.kind().name());

      Notification N6 = n3.getNotification();
      Assert.assertEquals(key, N6.getData());
      Assert.assertEquals(op2, N6.kind().name());
    } finally {
      stopServers();
    }
  }
}
