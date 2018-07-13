package mmux.kv;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import mmux.kv.BlockClient.CommandResponse;
import mmux.kv.BlockClient.CommandResponseReader;
import mmux.kv.BlockNameParser.BlockMetadata;
import org.apache.thrift.TException;

public class ReplicaChainClient implements Closeable {

  public class LockedClient implements Closeable {

    private ReplicaChainClient parent;
    private boolean redirecting;
    private List<String> redirectChain;

    LockedClient(ReplicaChainClient parent) throws TException {
      this.parent = parent;
      String response = StandardCharsets.UTF_8
          .decode(runCommand(KVOps.LOCK, Collections.emptyList()).get(0)).toString();
      if (!response.equals("!ok")) {
        this.redirecting = true;
        String[] parts = response.split("!");
        this.redirectChain = new ArrayList<>(parts.length - 1);
        this.redirectChain.addAll(Arrays.asList(parts).subList(2, parts.length));
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

    public boolean isRedirecting() {
      return redirecting;
    }

    public List<String> getRedirectChain() {
      return redirectChain;
    }

    public List<String> getChain() {
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

  private sequence_id seq;
  private List<String> chain;
  private BlockClient head;
  private BlockClient tail;
  private CommandResponseReader responseReader;
  private BlockClientCache cache;
  private boolean inFlight;

  ReplicaChainClient(BlockClientCache cache, List<String> chain) throws TException {
    if (chain == null || chain.size() == 0) {
      throw new IllegalArgumentException("Chain length must be >= 1");
    }
    this.cache = cache;
    this.chain = chain;
    this.inFlight = false;
    this.seq = new sequence_id(-1, 0, -1);
    connect();
  }

  @Override
  public void close() {
    head.close();
    tail.close();
  }

  public List<String> getChain() {
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
    CommandResponse response = responseReader.recieveResponse();

    if (response.clientSeqNo != seq.getClientSeqNo()) {
      throw new IllegalStateException(
          "SEQ: Expected=" + seq.getClientSeqNo() + " Received=" + response.clientSeqNo);
    }
    seq.client_seq_no++;
    inFlight = false;
    return response.result;
  }

  List<ByteBuffer> runCommand(BlockClient client, int cmdId, List<ByteBuffer> args)
      throws TException {
    sendCommandRequest(client, cmdId, args);
    return receiveCommandResponse();
  }

  List<ByteBuffer> runCommand(int cmdId, List<ByteBuffer> args) throws TException {
    if (KVOpType.opType(cmdId) == KVOpType.accessor) {
      return runCommand(tail, cmdId, args);
    } else {
      return runCommand(head, cmdId, args);
    }
  }

  private List<ByteBuffer> runCommandRedirected(BlockClient client, int cmdId,
      List<ByteBuffer> args)
      throws TException {
    List<ByteBuffer> newArgs = new ArrayList<>(args);
    newArgs.add(ByteBuffer.wrap("!redirected".getBytes()));
    return runCommand(client, cmdId, newArgs);
  }

  List<ByteBuffer> runCommandRedirected(int cmdId, List<ByteBuffer> args) throws TException {
    if (KVOpType.opType(cmdId) == KVOpType.accessor) {
      return runCommandRedirected(tail, cmdId, args);
    } else {
      return runCommandRedirected(head, cmdId, args);
    }
  }

  BlockClient getHead() {
    return head;
  }

  BlockClient getTail() {
    return tail;
  }

  private void connect() throws TException {
    BlockMetadata h = BlockNameParser.parse(chain.get(0));
    this.head = new BlockClient(cache, h.getHost(), h.getServicePort(), h.getBlockId());
    this.seq.setClientId(this.head.getClientId());
    if (chain.size() == 1) {
      this.tail = this.head;
    } else {
      BlockMetadata t = BlockNameParser.parse(chain.get(chain.size() - 1));
      this.tail = new BlockClient(cache, t.getHost(), t.getServicePort(), t.getBlockId());
    }
    this.responseReader = this.tail.newCommandResponseReader(seq.getClientId());
  }
}
