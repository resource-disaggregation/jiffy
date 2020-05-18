package jiffy.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import jiffy.storage.block_response_service.response_args;
import jiffy.util.ByteBufferUtils;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BlockClient {

  class CommandResponse {
    long clientSeqNo;
    public List<ByteBuffer> result;

    CommandResponse(long clientSeqNo, List<ByteBuffer> result) {
      this.clientSeqNo = clientSeqNo;
      this.result = result;
    }
  }

  class CommandResponseReader {

    private TProtocol protocol;

    CommandResponseReader(TProtocol protocol) {
      this.protocol = protocol;
    }

    CommandResponse receiveResponse() throws TException {
      TMessage message = protocol.readMessageBegin();
      if (message.type == TMessageType.EXCEPTION) {
        TApplicationException exception = new TApplicationException();
        exception.read(protocol);
        protocol.readMessageEnd();
        throw exception;
      }
      block_response_service.response_args args = new response_args();
      args.read(protocol);
      protocol.readMessageEnd();
      if (args.isSetSeq() && args.isSetResult()) {
        return new CommandResponse(args.getSeq().getClientSeqNo(), args.getResult());
      }
      throw new TApplicationException(TApplicationException.MISSING_RESULT,
          "Command response failure: unknown result");
    }
  }

  private int blockId;
  private Integer sequenceId;
  private TTransport transport;
  private TProtocol protocol;
  private block_request_service.Client client;

  BlockClient(BlockClientCache cache, String host, int port, int blockId, Integer sequenceId)
      throws TTransportException {
    BlockClientCache.Value value = cache.get(host, port);
    this.transport = value.getTransport();
    this.protocol = value.getProtocol();
    this.client = value.getClient();
    this.blockId = blockId;
    this.sequenceId = sequenceId;
  }

  public void close() {
    transport.close();
  }

  CommandResponseReader newCommandResponseReader(long clientId) throws TException {
    client.registerClientId(blockId, clientId);
    return new CommandResponseReader(protocol);
  }

  long getClientId() throws TException {
    return client.getClientId();
  }

  void sendCommandRequest(sequence_id seq, List<ByteBuffer> args) throws TException {
    System.out.println("sendCommand" + blockId);
    if(sequenceId != null) {
      System.out.println("seq:" + sequenceId);
      System.out.println("args:" + args.size());
      List<ByteBuffer> newArgs = new ArrayList<>(args);
      newArgs.add(ByteBufferUtils.fromString("seq:" + 1));
      args = newArgs;
    }
    client.commandRequest(seq, blockId, args);
  }
}