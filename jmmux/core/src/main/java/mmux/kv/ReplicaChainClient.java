package mmux.kv;

import com.github.phantomthief.thrift.client.ThriftClient;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import mmux.directory.directory_service;
import mmux.directory.rpc_replica_chain;
import mmux.directory.rpc_storage_mode;
import mmux.kv.BlockClient.CommandResponse;
import mmux.kv.BlockNameParser.BlockMetadata;
import mmux.util.ByteBufferUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

public class ReplicaChainClient implements Closeable {

  private static final int MAX_RETRIES = 3;

  public class LockedClient implements Closeable {

    private ReplicaChainClient parent;
    private boolean redirecting;
    private rpc_replica_chain redirectChain;

    LockedClient(ReplicaChainClient parent) throws TException {
      this.parent = parent;
      String response = StandardCharsets.UTF_8
          .decode(runCommand(KVOps.LOCK, Collections.emptyList()).get(0)).toString();
      if (!response.equals("!ok")) {
        this.redirecting = true;
        String[] parts = response.split("!");
        this.redirectChain = new rpc_replica_chain(new ArrayList<>(parts.length - 1), 0, 0,
            rpc_storage_mode.rpc_in_memory);
        this.redirectChain.block_names.addAll(Arrays.asList(parts).subList(2, parts.length));
      } else {
        this.redirecting = false;
        this.redirectChain = null;
      }
    }

    void unlock() throws TException {
      runCommand(KVOps.UNLOCK, Collections.emptyList());
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

  private ThriftClient fs;
  private String path;
  private sequence_id seq;
  private rpc_replica_chain chain;
  private BlockClient head;
  private BlockClient tail;
  private BlockClientCache cache;
  private boolean inFlight;

  ReplicaChainClient(ThriftClient fs, String path, BlockClientCache cache, rpc_replica_chain chain)
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

  public directory_service.Iface fs() {
    return fs.iface(directory_service.Client.class, TBinaryProtocol::new, 0);
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
    if (KVOpType.opType(cmdId) == KVOpType.accessor) {
      sendCommandRequest(tail, cmdId, args);
    } else {
      sendCommandRequest(head, cmdId, args);
    }
  }

  List<ByteBuffer> receiveCommandResponse() throws TException {
    CommandResponse response = tail.recieveCommandResponse();

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
    int numTries = 0;
    while (response == null) {
      try {
        if (KVOpType.opType(cmdId) == KVOpType.accessor) {
          response = runCommand(tail, cmdId, args);
        } else {
          response = runCommand(head, cmdId, args);
          if (retry && ByteBufferUtils.toString(response.get(0).slice()).equals("!duplicate_key")) {
            response.set(0, ByteBufferUtils.fromString("!ok"));
          }
        }
      } catch (TTransportException e) {
        chain = fs().resloveFailures(path, chain);
        invalidateCache();
        connect();
        retry = true;
        if (++numTries > MAX_RETRIES) {
          throw e;
        }
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
    if (KVOpType.opType(cmdId) == KVOpType.accessor) {
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
    this.tail.registerClientId(seq.getClientId());
    this.inFlight = false;
  }

  private void invalidateCache() {
    for (String block : chain.block_names) {
      BlockMetadata m = BlockNameParser.parse(block);
      cache.remove(m.getHost(), m.getServicePort());
    }
  }
}
