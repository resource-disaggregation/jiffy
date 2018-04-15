package elasticmem.kv;

import elasticmem.kv.BlockClient.CommandResponse;
import elasticmem.kv.BlockClient.CommandResponseReader;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import org.apache.thrift.TException;

public class ChainClient implements Closeable {
  private sequence_id seq;
  private BlockClient head;
  private BlockClient tail;
  private CommandResponseReader responseReader;
  private HashMap<Long, List<ByteBuffer>> responseCache;

  ChainClient(List<String> chain) throws TException {
    if (chain == null || chain.size() == 0) {
      throw new IllegalArgumentException("Chain length must be >= 1");
    }
    this.seq = new sequence_id(-1, 0, -1);
    String[] headPts = chain.get(0).split(":");
    this.head = new BlockClient(headPts[0], Integer.parseInt(headPts[1]), Integer.parseInt(headPts[5]));
    this.seq.setClientId(this.head.getClientId());
    if (chain.size() == 1) {
      this.tail = this.head;
    } else {
      String[] tailPts = chain.get(chain.size() - 1).split(":");
      this.tail = new BlockClient(tailPts[0], Integer.parseInt(tailPts[1]), Integer.parseInt(tailPts[5]));
    }
    this.responseReader = this.tail.newCommandResponseReader(seq.getClientId());
    this.responseCache = new HashMap<>();
  }

  @Override
  public void close() {
    head.close();
    tail.close();
  }

  long sendCommandRequest(BlockClient client, int cmdId, List<ByteBuffer> args)
      throws TException {
    long opSeq = seq.getClientSeqNo();
    client.sendCommandRequest(seq, cmdId, args);
    seq.client_seq_no++;
    return opSeq;
  }

  List<ByteBuffer> receiveCommandResponse(long opSeq) throws TException {
    if (responseCache.containsKey(opSeq)) {
      return responseCache.remove(opSeq);
    }

    while (true) {
      CommandResponse response = responseReader.recieveResponse();
      if (opSeq == response.clientSeqNo) {
        return response.result;
      } else {
        responseCache.put(response.clientSeqNo, response.result);
      }
    }
  }

  List<ByteBuffer> runCommand(BlockClient client, int cmdId, List<ByteBuffer> args)
      throws TException {
    long opSeq = sendCommandRequest(client, cmdId, args);
    return receiveCommandResponse(opSeq);
  }

  BlockClient getHead() {
    return head;
  }

  BlockClient getTail() {
    return tail;
  }
}
