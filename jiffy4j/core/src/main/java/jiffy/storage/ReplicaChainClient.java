package jiffy.storage;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import jiffy.directory.directory_service.Client;
import jiffy.directory.rpc_replica_chain;
import jiffy.storage.BlockClient.CommandResponse;
import jiffy.storage.BlockClient.CommandResponseReader;
import jiffy.storage.BlockNameParser.BlockMetadata;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class ReplicaChainClient implements Closeable {

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
    if (chain == null || chain.block_ids.size() == 0) {
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
    CommandResponse response = responseReader.receiveResponse();

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
    int numRetriesRemaining = 3;
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
        if (numRetriesRemaining > 0) {
          chain = fs.resloveFailures(path, chain);
          invalidateCache();
          connect();
          retry = true;
          numRetriesRemaining--;
        } else {
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
    if (CommandType.opType(cmdId) == CommandType.accessor) {
      return runCommandRedirected(tail, cmdId, args);
    } else {
      return runCommandRedirected(head, cmdId, args);
    }
  }

  private void connect() throws TException {
    this.seq = new sequence_id(-1, 0, -1);
    BlockMetadata h = BlockNameParser.parse(chain.block_ids.get(0));
    this.head = new BlockClient(cache, h.getHost(), h.getServicePort(), h.getBlockId());
    this.seq.setClientId(this.head.getClientId());
    if (chain.block_ids.size() == 1) {
      this.tail = this.head;
    } else {
      BlockMetadata t = BlockNameParser.parse(chain.block_ids.get(chain.block_ids.size() - 1));
      this.tail = new BlockClient(cache, t.getHost(), t.getServicePort(), t.getBlockId());
    }
    this.responseReader = this.tail.newCommandResponseReader(seq.getClientId());
    this.inFlight = false;
  }

  private void invalidateCache() {
    for (String block : chain.block_ids) {
      BlockMetadata m = BlockNameParser.parse(block);
      cache.remove(m.getHost(), m.getServicePort());
    }
  }
}
