package jiffy.storage;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_replica_chain;
import jiffy.directory.rpc_storage_mode;
import jiffy.storage.BlockClient.CommandResponse;
import jiffy.storage.BlockClient.CommandResponseReader;
import jiffy.storage.BlockNameParser.BlockMetadata;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class ReplicaChainClient implements Closeable {

  public class LockedClient implements Closeable {

    private ReplicaChainClient parent;
    private boolean redirecting;
    private rpc_replica_chain redirectChain;

    LockedClient(ReplicaChainClient parent) throws TException {
      this.parent = parent;
      String response = StandardCharsets.UTF_8
          .decode(runCommand(HashTableOps.LOCK, Collections.emptyList()).get(0)).toString();
      if (!response.equals("!ok")) {
        this.redirecting = true;
        String[] parts = response.split("!");
        this.redirectChain = new rpc_replica_chain(new ArrayList<>(parts.length - 1), parent.getChain().getName(), parent.getChain().getMetadata(),
            rpc_storage_mode.rpc_in_memory);
        this.redirectChain.block_names.addAll(Arrays.asList(parts).subList(2, parts.length));
      } else {
        this.redirecting = false;
        this.redirectChain = null;
      }
    }

    void unlock() throws TException {
      runCommand(HashTableOps.UNLOCK, Collections.emptyList());
    }

    void sendCommandRequest(int cmdId, List<ByteBuffer> args) throws TException {
      parent.sendCommandRequest(cmdId, args);
    }

    List<ByteBuffer> receiveCommandResponse() throws TException {
      return parent.receiveCommandResponse();
    }

    List<ByteBuffer> runCommand(int cmdId, List<ByteBuffer> args) throws TException {
      return parent.runCommand(cmdId, args);
    }

    List<ByteBuffer> runCommandRedirected(int cmdId, List<ByteBuffer> args) throws TException {
      return parent.runCommandRedirected(cmdId, args);
    }

    boolean isRedirecting() {
      return redirecting;
    }

    rpc_replica_chain getRedirectChain() {
      return redirectChain;
    }

    public rpc_replica_chain getChain() {
      return parent.getChain();
    }

    @Override
    public void close() {
      try {
        unlock();
      } catch (TException ignored) {
      }
    }
  }

  private Client fs;
  private String path;
  private sequence_id seq;
  private rpc_replica_chain chain;
  private BlockClient head;
  private BlockClient tail;
  private CommandResponseReader responseReader;
  private BlockClientCache cache;
  private boolean inFlight;

  ReplicaChainClient(Client fs, String path, BlockClientCache cache, rpc_replica_chain chain)
      throws TException {
    if (chain == null || chain.block_names.size() == 0) {
      throw new IllegalArgumentException("Chain length must be >= 1");
    }
    this.fs = fs;
    this.path = path;
    this.cache = cache;
    this.chain = chain;
    connect();
  }

  @Override
  public void close() {
    head.close();
    tail.close();
  }

  public rpc_replica_chain getChain() {
    return chain;
  }

  public LockedClient lock() throws TException {
    return new LockedClient(this);
  }

  private void sendCommandRequest(BlockClient client, int cmdId, List<ByteBuffer> args)
      throws TException {
    if (inFlight) {
      throw new IllegalStateException("Cannot have more than one request in-flight");
    }
    client.sendCommandRequest(seq, cmdId, args);
    inFlight = true;
  }

  void sendCommandRequest(int cmdId, List<ByteBuffer> args) throws TException {
    if (CommandType.opType(cmdId) == CommandType.accessor) {
      sendCommandRequest(tail, cmdId, args);
    } else {
      sendCommandRequest(head, cmdId, args);
    }
  }

  List<ByteBuffer> receiveCommandResponse() throws TException {
    CommandResponse response = responseReader.recieveResponse();

    if (response.clientSeqNo != seq.getClientSeqNo()) {
      throw new IllegalStateException(
          "SEQ: Expected=" + seq.getClientSeqNo() + " Received=" + response.clientSeqNo);
    }
    seq.client_seq_no++;
    inFlight = false;
    return response.result;
  }

  private List<ByteBuffer> runCommand(BlockClient client, int cmdId, List<ByteBuffer> args)
      throws TException {
    sendCommandRequest(client, cmdId, args);
    return receiveCommandResponse();
  }

  List<ByteBuffer> runCommand(int cmdId, List<ByteBuffer> args) throws TException {
    List<ByteBuffer> response = null;
    boolean retry = false;
    while (response == null) {
      try {
        if (CommandType.opType(cmdId) == CommandType.accessor) {
          response = runCommand(tail, cmdId, args);
        } else {
          response = runCommand(head, cmdId, args);
          if (retry && ByteBufferUtils.toString(response.get(0)).equals("!duplicate_key")) {
            response.set(0, ByteBufferUtils.fromString("!ok"));
          }
        }
      } catch (TTransportException e) {
        chain = fs.resloveFailures(path, chain);
        invalidateCache();
        connect();
        retry = true;
      }
    }
    return response;
  }

  private List<ByteBuffer> runCommandRedirected(BlockClient client, int cmdId,
      List<ByteBuffer> args)
      throws TException {
    List<ByteBuffer> newArgs = new ArrayList<>(args);
    newArgs.add(ByteBufferUtils.fromString("!redirected"));
    return runCommand(client, cmdId, newArgs);
  }

  List<ByteBuffer> runCommandRedirected(int cmdId, List<ByteBuffer> args) throws TException {
    if (CommandType.opType(cmdId) == CommandType.accessor) {
      return runCommandRedirected(tail, cmdId, args);
    } else {
      return runCommandRedirected(head, cmdId, args);
    }
  }

  private void connect() throws TException {
    this.seq = new sequence_id(-1, 0, -1);
    BlockMetadata h = BlockNameParser.parse(chain.block_names.get(0));
    this.head = new BlockClient(cache, h.getHost(), h.getServicePort(), h.getBlockId());
    this.seq.setClientId(this.head.getClientId());
    if (chain.block_names.size() == 1) {
      this.tail = this.head;
    } else {
      BlockMetadata t = BlockNameParser.parse(chain.block_names.get(chain.block_names.size() - 1));
      this.tail = new BlockClient(cache, t.getHost(), t.getServicePort(), t.getBlockId());
    }
    this.responseReader = this.tail.newCommandResponseReader(seq.getClientId());
    this.inFlight = false;
  }

  private void invalidateCache() {
    for (String block : chain.block_names) {
      BlockMetadata m = BlockNameParser.parse(block);
      cache.remove(m.getHost(), m.getServicePort());
    }
  }
}
