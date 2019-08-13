package jiffy.storage;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import jiffy.directory.rpc_replica_chain;
import jiffy.directory.rpc_storage_mode;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class HashTableClient extends DataStructureClient implements Closeable {

  private int[] slots;
  private ReplicaChainClient[] blocks;
  private int redoTimes;

  public HashTableClient(Client fs, String path, rpc_data_status dataStatus, int timeoutMs)
      throws TException {
    super(fs, path, dataStatus, timeoutMs);
    init();
  }

  private void init() throws TException {
    this.redoTimes = 0;
    this.blocks = new ReplicaChainClient[dataStatus.data_blocks.size()];
    this.slots = new int[dataStatus.data_blocks.size()];
    for (int i = 0; i < blocks.length; i++) {
      slots[i] = Integer.parseInt(dataStatus.data_blocks.get(i).name.split("_")[0]);
      blocks[i] = new ReplicaChainClient(fs, path, cache, dataStatus.data_blocks.get(i),
          HashTableCommands.CMD_TYPES);
    }
  }

  @Override
  public void close() {
    for (ReplicaChainClient client : blocks) {
      client.close();
    }
  }

  public rpc_data_status getDataStatus() {
    return dataStatus;
  }

  void refresh() throws TException {
    this.dataStatus = fs.dstatus(path);
    init();
  }

  ByteBuffer handleRedirect(List<ByteBuffer> args, ByteBuffer response)
      throws TException {
    String resp;
    while ((resp = ByteBufferUtils.toString(response)).startsWith("!exporting")) {
      rpc_replica_chain chain = extractChain(resp);
      response = new ReplicaChainClient(fs, path, cache, chain, HashTableCommands.CMD_TYPES)
          .runCommandRedirected(args)
          .get(0);
    }
    if (resp.equals("!block_moved")) {
      refresh();
      return null;
    }
    if (resp.equals("!full")) {
      long sleepyTime = (long) Math.pow(2, redoTimes);
      try {
        Thread.sleep(sleepyTime);
        redoTimes++;
      } catch (InterruptedException ignored) {}
      return null;
    }
    return response;
  }

  public boolean exists(ByteBuffer key) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(HashTableCommands.EXISTS, key);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return ByteBufferUtils.toString(response).equals("true");
  }

  public ByteBuffer get(ByteBuffer key) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(HashTableCommands.GET, key);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }

  public ByteBuffer put(ByteBuffer key, ByteBuffer value) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(HashTableCommands.PUT, key, value);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }

  public ByteBuffer upsert(ByteBuffer key, ByteBuffer value) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(HashTableCommands.UPSERT, key, value);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }

  public ByteBuffer update(ByteBuffer key, ByteBuffer value) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(HashTableCommands.UPDATE, key, value);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }

  public ByteBuffer remove(ByteBuffer key) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(HashTableCommands.REMOVE, key);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(args).get(0);
      response = handleRedirect(args, response);
    }
    return response;
  }

  private int blockId(ByteBuffer key) {
    return findSlot(HashSlot.get(key));
  }

  private int findSlot(int hash) {
    int low = 0;
    int high = slots.length;
    while (low < high) {
      final int mid = (low + high) / 2;
      if (hash >= slots[mid]) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low - 1;
  }

  private rpc_replica_chain extractChain(String msg) {
    String[] parts = msg.split("!");
    List<String> chain = new ArrayList<>(parts.length - 1);
    chain.addAll(Arrays.asList(parts).subList(2, parts.length));
    return new rpc_replica_chain(chain, "", "", rpc_storage_mode.rpc_in_memory);
  }
}
