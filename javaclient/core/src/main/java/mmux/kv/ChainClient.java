package mmux.kv;

import java.util.Arrays;
import mmux.kv.BlockClient.CommandResponse;
import mmux.kv.BlockClient.CommandResponseReader;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.List;
import mmux.kv.BlockNameParser.BlockMetadata;
import org.apache.thrift.TException;

public class ChainClient implements Closeable {

  private sequence_id seq;
  private List<String> chain;
  private BlockClient head;
  private BlockClient tail;
  private CommandResponseReader responseReader;
  private BlockClientCache cache;
  private boolean inFlight;

  ChainClient(BlockClientCache cache, List<String> chain) throws TException {
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

  private void sendCommandRequest(BlockClient client, List<Integer> cmdId,
      List<List<ByteBuffer>> args)
      throws TException {
    if (inFlight) {
      throw new IllegalStateException("Cannot have more than one request in-flight");
    }
    client.sendCommandRequest(seq, cmdId, args);
    inFlight = true;
  }

  private List<List<ByteBuffer>> receiveCommandResponse() throws TException {
    CommandResponse response = responseReader.recieveResponse();

    if (response.clientSeqNo != seq.getClientSeqNo()) {
      throw new IllegalStateException(
          "SEQ: Expected=" + seq.getClientSeqNo() + " Received=" + response.clientSeqNo);
    }
    seq.client_seq_no++;
    inFlight = false;
    return response.result;
  }

  List<List<ByteBuffer>> runCommand(BlockClient client, List<Integer> cmdId,
      List<List<ByteBuffer>> args)
      throws TException {
    sendCommandRequest(client, cmdId, args);
    return receiveCommandResponse();
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
