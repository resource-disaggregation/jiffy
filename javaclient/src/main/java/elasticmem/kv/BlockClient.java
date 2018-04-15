package elasticmem.kv;

import elasticmem.kv.block_request_service.Client;
import elasticmem.kv.block_response_service.response_args;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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

    CommandResponse recieveResponse() throws TException {
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

  private int id;
  private TTransport transport;
  private TProtocol protocol;
  private block_request_service.Client client;

  BlockClient(String host, int port, int blockId) {
    this.transport = new TSocket(host, port);
    this.protocol = new TBinaryProtocol(transport);
    this.client = new Client(protocol);
    this.id = blockId;
  }

  void close() {
    transport.close();
  }

  CommandResponseReader newCommandResponseReader(long clientId) throws TException {
    client.registerClientId(id, clientId);
    return new CommandResponseReader(protocol);
  }

  long getClientId() throws TException {
    return client.getClientId();
  }

  void sendCommandRequest(sequence_id seq, int cmdId, List<ByteBuffer> args) throws TException {
    client.commandRequest(seq, id, cmdId, args);
  }
}
