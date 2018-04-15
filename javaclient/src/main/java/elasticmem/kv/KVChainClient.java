package elasticmem.kv;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.TException;

public class KVChainClient extends ChainClient {

  KVChainClient(List<String> chain) throws TException {
    super(chain);
  }

  public ByteBuffer get(ByteBuffer key) throws TException {
    return runCommand(getTail(), 0, Collections.singletonList(key)).get(0);
  }

  public long pipelineGet(ByteBuffer key) throws TException {
    return sendCommandRequest(getTail(), 0, Collections.singletonList(key));
  }

  public ByteBuffer put(ByteBuffer key, ByteBuffer value) throws TException {
    return runCommand(getHead(), 1, Arrays.asList(key, value)).get(0);
  }

  public long pipelinePut(ByteBuffer key, ByteBuffer value) throws TException {
    return sendCommandRequest(getHead(), 1, Arrays.asList(key, value));
  }

  public ByteBuffer update(ByteBuffer key, ByteBuffer value) throws TException {
    return runCommand(getHead(), 2, Arrays.asList(key, value)).get(0);
  }

  public long pipelineUpdate(ByteBuffer key, ByteBuffer value) throws TException {
    return sendCommandRequest(getHead(), 2, Arrays.asList(key, value));
  }

  public ByteBuffer remove(ByteBuffer key) throws TException {
    return runCommand(getHead(), 3, Collections.singletonList(key)).get(0);
  }

  public long pipelineRemove(ByteBuffer key) throws TException {
    return sendCommandRequest(getHead(), 3, Collections.singletonList(key));
  }

  public ByteBuffer receiveResponse(long opSeq) throws TException {
    return receiveCommandResponse(opSeq).get(0);
  }
}
