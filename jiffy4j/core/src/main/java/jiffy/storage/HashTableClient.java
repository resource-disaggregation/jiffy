package jiffy.storage;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jiffy.directory.directory_service;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_data_status;
import jiffy.directory.rpc_replica_chain;
import jiffy.directory.rpc_storage_mode;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;

public class HashTableClient implements Closeable {

  public class LockedClient implements Closeable {

    private HashTableClient parent;
    private ReplicaChainClient.LockedClient[] blocks;
    private ReplicaChainClient[] redirectBlocks;
    private ReplicaChainClient.LockedClient[] lockedRedirectBlocks;
    private ReplicaChainClient.LockedClient[] newBlocks;

    LockedClient(HashTableClient parent) throws TException {
      this.parent = parent;
      this.blocks = new ReplicaChainClient.LockedClient[parent.blocks.length];
      this.redirectBlocks = new ReplicaChainClient[parent.blocks.length];
      this.lockedRedirectBlocks = new ReplicaChainClient.LockedClient[parent.blocks.length];
      this.newBlocks = new ReplicaChainClient.LockedClient[parent.blocks.length];

      for (int i = 0; i < blocks.length; i++) {
        blocks[i] = parent.blocks[i].lock();
      }

      for (int i = 0; i < blocks.length; i++) {
        if (blocks[i].isRedirecting()) {
          boolean newBlock = true;
          for (int j = 0; j < blocks.length; j++) {
            if (blocks[i].getRedirectChain().equals(blocks[j].getChain())) {
              newBlock = false;
              redirectBlocks[i] = parent.blocks[j];
              lockedRedirectBlocks[i] = blocks[j];
              break;
            }
          }
          if (newBlock) {
            redirectBlocks[i] = new ReplicaChainClient(fs, path, parent.cache,
                blocks[i].getRedirectChain());
            lockedRedirectBlocks[i] = redirectBlocks[i].lock();
            newBlocks[i] = lockedRedirectBlocks[i];
          }
        } else {
          newBlocks[i] = null;
          redirectBlocks[i] = null;
          lockedRedirectBlocks[i] = null;
        }
      }
    }

    private ByteBuffer handleRedirect(int cmdId, List<ByteBuffer> args, ByteBuffer response)
        throws TException {
      String resp;
      while ((resp = ByteBufferUtils.toString(response)).startsWith("!exporting")) {
        rpc_replica_chain chain = parent.extractChain(resp);
        boolean found = false;
        for (ReplicaChainClient.LockedClient block : blocks) {
          if (block.getChain().block_names.equals(chain.block_names)) {
            found = true;
            response = block.runCommandRedirected(cmdId, args).get(0);
            break;
          }
        }
        if (!found) {
          response = new ReplicaChainClient(fs, path, cache, chain)
              .runCommandRedirected(cmdId, args)
              .get(0);
        }
      }
      return response;
    }

    private List<ByteBuffer> handleRedirects(int cmdId, List<ByteBuffer> args,
        List<ByteBuffer> responses) throws TException {
      int numOps = responses.size();
      int numOpArgs = args.size() / numOps;
      for (int i = 0; i < numOps; i++) {
        ByteBuffer response = responses.get(i);
        String resp;
        while ((resp = ByteBufferUtils.toString(response)).startsWith("!exporting")) {
          rpc_replica_chain chain = parent.extractChain(resp);
          List<ByteBuffer> opArgs = args.subList(i * numOpArgs, (i + 1) * numOpArgs);
          boolean found = false;
          for (ReplicaChainClient.LockedClient block : blocks) {
            if (block.getChain().block_names.equals(chain.block_names)) {
              found = true;
              response = block.runCommandRedirected(cmdId, args).get(0);
              break;
            }
          }
          if (!found) {
            response = new ReplicaChainClient(fs, path, cache, chain)
                .runCommandRedirected(cmdId, opArgs).get(0);
          }
        }
        responses.set(i, response);
      }
      return responses;
    }

    @Override
    public void close() {
      try {
        for (int i = 0; i < blocks.length; i++) {
          blocks[i].unlock();
          if (newBlocks[i] != null) {
            newBlocks[i].unlock();
          }
        }
      } catch (TException ignored) {
      }
    }

    public ByteBuffer get(ByteBuffer key) throws TException {
      List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key);
      ByteBuffer response = blocks[blockId(key)].runCommand(HashTableOps.LOCKED_GET, args).get(0);
      return handleRedirect(HashTableOps.LOCKED_GET, args, response);
    }

    public ByteBuffer put(ByteBuffer key, ByteBuffer value) throws TException {
      List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key, value);
      ByteBuffer response = blocks[blockId(key)].runCommand(HashTableOps.LOCKED_PUT, args).get(0);
      return handleRedirect(HashTableOps.LOCKED_PUT, args, response);
    }

    public ByteBuffer upsert(ByteBuffer key, ByteBuffer value) throws TException {
      List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key, value);
      ByteBuffer response = blocks[blockId(key)].runCommand(HashTableOps.LOCKED_UPSERT, args).get(0);
      return handleRedirect(HashTableOps.LOCKED_UPSERT, args, response);
    }

    public ByteBuffer update(ByteBuffer key, ByteBuffer value) throws TException {
      List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key, value);
      ByteBuffer response = blocks[blockId(key)].runCommand(HashTableOps.LOCKED_UPDATE, args).get(0);
      return handleRedirect(HashTableOps.LOCKED_UPDATE, args, response);
    }

    public ByteBuffer remove(ByteBuffer key) throws TException {
      List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key);
      ByteBuffer response = blocks[blockId(key)].runCommand(HashTableOps.LOCKED_REMOVE, args).get(0);
      return handleRedirect(HashTableOps.LOCKED_REMOVE, args, response);
    }

    public List<ByteBuffer> get(List<ByteBuffer> args) throws TException {
      List<ByteBuffer> response = parent.batchCommand(HashTableOps.LOCKED_GET, args, 1);
      return handleRedirects(HashTableOps.LOCKED_GET, args, response);
    }

    public List<ByteBuffer> put(List<ByteBuffer> args) throws TException {
      List<ByteBuffer> response = parent.batchCommand(HashTableOps.LOCKED_PUT, args, 2);
      return handleRedirects(HashTableOps.LOCKED_PUT, args, response);
    }

    public List<ByteBuffer> upsert(List<ByteBuffer> args) throws TException {
      List<ByteBuffer> response = parent.batchCommand(HashTableOps.LOCKED_UPSERT, args, 2);
      return handleRedirects(HashTableOps.LOCKED_UPSERT, args, response);
    }

    public List<ByteBuffer> update(List<ByteBuffer> args) throws TException {
      List<ByteBuffer> response = parent.batchCommand(HashTableOps.LOCKED_UPDATE, args, 2);
      return handleRedirects(HashTableOps.LOCKED_UPDATE, args, response);
    }

    public List<ByteBuffer> remove(List<ByteBuffer> args) throws TException {
      List<ByteBuffer> response = parent.batchCommand(HashTableOps.LOCKED_REMOVE, args, 1);
      return handleRedirects(HashTableOps.LOCKED_REMOVE, args, response);
    }

    public long numKeys() throws TException {
      for (int i = 0; i < blocks.length; i++) {
        blocks[i].sendCommandRequest(HashTableOps.NUM_KEYS, ByteBufferUtils.fromByteBuffers());
        if (newBlocks[i] != null) {
          newBlocks[i].sendCommandRequest(HashTableOps.NUM_KEYS, ByteBufferUtils.fromByteBuffers());
        }
      }
      long n = 0;
      for (int i = 0; i < blocks.length; i++) {
        n += Long.parseLong(ByteBufferUtils.toString(blocks[i].receiveCommandResponse().get(0)));
        if (newBlocks[i] != null) {
          n += Long
              .parseLong(ByteBufferUtils.toString(newBlocks[i].receiveCommandResponse().get(0)));
        }
      }
      return n;
    }
  }

  private int[] slots;
  private ReplicaChainClient[] blocks;
  private directory_service.Client fs;
  private String path;
  private BlockClientCache cache;
  private rpc_data_status dataStatus;

  public HashTableClient(Client fs, String path, rpc_data_status dataStatus, int timeoutMs)
      throws TException {
    this.fs = fs;
    this.path = path;
    this.dataStatus = dataStatus;
    this.blocks = new ReplicaChainClient[dataStatus.data_blocks.size()];
    this.slots = new int[dataStatus.data_blocks.size()];
    this.cache = new BlockClientCache(timeoutMs);
    for (int i = 0; i < blocks.length; i++) {
      slots[i] = Integer.parseInt(dataStatus.data_blocks.get(i).name.split("_")[0]);
      blocks[i] = new ReplicaChainClient(fs, path, cache, dataStatus.data_blocks.get(i));
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

  private void refresh() throws TException {
    this.dataStatus  = fs.dstatus(path);
    this.blocks = new ReplicaChainClient[dataStatus.data_blocks.size()];
    this.slots = new int[dataStatus.data_blocks.size()];
    for (int i = 0; i < blocks.length; i++) {
      slots[i] = Integer.parseInt(dataStatus.data_blocks.get(i).name.split("_")[0]);
      blocks[i] = new ReplicaChainClient(fs, path, cache, dataStatus.data_blocks.get(i));
    }
  }

  public LockedClient lock() throws TException {
    return new LockedClient(this);
  }

  private ByteBuffer handleRedirect(int cmdId, List<ByteBuffer> args, ByteBuffer response)
      throws TException {
    String resp;
    while ((resp = ByteBufferUtils.toString(response)).startsWith("!exporting")) {
      rpc_replica_chain chain = extractChain(resp);
      response = new ReplicaChainClient(fs, path, cache, chain).runCommandRedirected(cmdId, args).get(0);
    }
    if (ByteBufferUtils.toString(response).equals("!block_moved")) {
      refresh();
      return null;
    }
    return (ByteBuffer) response.rewind();
  }

  private List<ByteBuffer> handleRedirects(int cmdId, List<ByteBuffer> args,
      List<ByteBuffer> responses) throws TException {
    int numOps = responses.size();
    int numOpArgs = args.size() / numOps;
    for (int i = 0; i < numOps; i++) {
      ByteBuffer response = responses.get(i);
      String resp;
      while ((resp = ByteBufferUtils.toString(response)).startsWith("!exporting")) {
        rpc_replica_chain chain = extractChain(resp);
        List<ByteBuffer> opArgs = args.subList(i * numOpArgs, (i + 1) * numOpArgs);
        response = new ReplicaChainClient(fs, path, cache, chain).runCommandRedirected(cmdId, opArgs)
            .get(0);
      }
      if (ByteBufferUtils.toString(response).equals("!block_moved")) {
        refresh();
        return null;
      }
      responses.set(i, (ByteBuffer) response.rewind());
    }
    return responses;
  }

  private List<ByteBuffer> batchCommand(int op, List<ByteBuffer> args, int argsPerOp)
      throws TException {
    if (args.size() % argsPerOp != 0) {
      throw new IllegalArgumentException("Incorrect number of arguments");
    }

    int numOps = args.size() / argsPerOp;
    List<List<ByteBuffer>> blockArgs = new ArrayList<>(blocks.length);
    List<List<Integer>> positions = new ArrayList<>(blocks.length);
    for (ReplicaChainClient ignored : blocks) {
      blockArgs.add(null);
      positions.add(null);
    }

    for (int i = 0; i < numOps; i++) {
      int id = blockId(args.get(i * argsPerOp));
      if (blockArgs.get(id) == null) {
        blockArgs.set(id, new ArrayList<>());
        positions.set(id, new ArrayList<>());
      }
      blockArgs.get(id).addAll(args.subList(i * argsPerOp, (i + 1) * argsPerOp));
      positions.get(id).add(i);
    }

    for (int i = 0; i < blocks.length; i++) {
      if (blockArgs.get(i) != null) {
        blocks[i].sendCommandRequest(op, blockArgs.get(i));
      }
    }

    List<ByteBuffer> responses = Arrays.asList(new ByteBuffer[numOps]);
    for (int i = 0; i < blocks.length; i++) {
      if (blockArgs.get(i) != null) {
        List<ByteBuffer> response = blocks[i].receiveCommandResponse();
        for (int j = 0; j < response.size(); j++) {
          responses.set(positions.get(i).get(j), response.get(j));
        }
      }
    }
    return responses;
  }

  public boolean exists(ByteBuffer key) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(HashTableOps.EXISTS, args).get(0);
      response = handleRedirect(HashTableOps.EXISTS, args, response);
    }
    return ByteBufferUtils.toString(response).equals("true");
  }

  public ByteBuffer get(ByteBuffer key) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(HashTableOps.GET, args).get(0);
      response = handleRedirect(HashTableOps.GET, args, response);
    }
    return response;
  }

  public ByteBuffer put(ByteBuffer key, ByteBuffer value) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key, value);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(HashTableOps.PUT, args).get(0);
      response = handleRedirect(HashTableOps.PUT, args, response);
    }
    return response;
  }

  public ByteBuffer upsert(ByteBuffer key, ByteBuffer value) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key, value);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(HashTableOps.UPSERT, args).get(0);
      response = handleRedirect(HashTableOps.UPSERT, args, response);
    }
    return response;
  }

  public ByteBuffer update(ByteBuffer key, ByteBuffer value) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key, value);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(HashTableOps.UPDATE, args).get(0);
      response = handleRedirect(HashTableOps.UPDATE, args, response);
    }
    return response;
  }

  public ByteBuffer remove(ByteBuffer key) throws TException {
    List<ByteBuffer> args = ByteBufferUtils.fromByteBuffers(key);
    ByteBuffer response = null;
    while (response == null) {
      response = blocks[blockId(key)].runCommand(HashTableOps.REMOVE, args).get(0);
      response = handleRedirect(HashTableOps.REMOVE, args, response);
    }
    return response;
  }

  public List<Boolean> exists(List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    while (response == null) {
      response = batchCommand(HashTableOps.EXISTS, args, 1);
      response = handleRedirects(HashTableOps.EXISTS, args, response);
    }
    List<Boolean> out = new ArrayList<>(response.size());
    for (ByteBuffer r : response) {
      out.add(ByteBufferUtils.toString(r).equals("true"));
    }
    return out;
  }

  public List<ByteBuffer> get(List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    while (response == null) {
      response = batchCommand(HashTableOps.GET, args, 1);
      response = handleRedirects(HashTableOps.GET, args, response);
    }
    return response;
  }

  public List<ByteBuffer> put(List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    while (response == null) {
      response = batchCommand(HashTableOps.PUT, args, 2);
      response = handleRedirects(HashTableOps.PUT, args, response);
    }
    return response;
  }

  public List<ByteBuffer> upsert(List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    while (response == null) {
      response = batchCommand(HashTableOps.UPSERT, args, 2);
      response = handleRedirects(HashTableOps.UPSERT, args, response);
    }
    return response;
  }

  public List<ByteBuffer> update(List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    while (response == null) {
      response = batchCommand(HashTableOps.UPDATE, args, 2);
      response = handleRedirects(HashTableOps.UPDATE, args, response);
    }
    return response;
  }

  public List<ByteBuffer> remove(List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    while (response == null) {
      response = batchCommand(HashTableOps.REMOVE, args, 1);
      response = handleRedirects(HashTableOps.REMOVE, args, response);
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
