package elasticmem.kv;

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
    return runCommand(getTail(), KVOps.GET, Collections.singletonList(key)).get(0);
  }

  public long pipelineGet(ByteBuffer key) throws TException {
    return sendCommandRequest(getTail(), KVOps.GET, Collections.singletonList(key));
  }

  public ByteBuffer put(ByteBuffer key, ByteBuffer value) throws TException {
    return runCommand(getHead(), KVOps.PUT, Arrays.asList(key, value)).get(0);
  }

  public long pipelinePut(ByteBuffer key, ByteBuffer value) throws TException {
    return sendCommandRequest(getHead(), KVOps.PUT, Arrays.asList(key, value));
  }

  public ByteBuffer update(ByteBuffer key, ByteBuffer value) throws TException {
    return runCommand(getHead(), KVOps.UPDATE, Arrays.asList(key, value)).get(0);
  }

  public long pipelineUpdate(ByteBuffer key, ByteBuffer value) throws TException {
    return sendCommandRequest(getHead(), KVOps.UPDATE, Arrays.asList(key, value));
  }

  public ByteBuffer remove(ByteBuffer key) throws TException {
    return runCommand(getHead(), KVOps.REMOVE, Collections.singletonList(key)).get(0);
  }

  public long pipelineRemove(ByteBuffer key) throws TException {
    return sendCommandRequest(getHead(), KVOps.REMOVE, Collections.singletonList(key));
  }

  public long numKeys() throws TException {
    String res = runCommand(getTail(), KVOps.NUM_KEYS, Collections.emptyList()).get(0).toString();
    if (res.equals("args_error")) {
      throw new TException("Incorrect number of arguments for numKeys");
    }
    return Long.parseLong(res);
  }

  public ByteBuffer receiveResponse(long opSeq) throws TException {
    return receiveCommandResponse(opSeq).get(0);
  }
}
