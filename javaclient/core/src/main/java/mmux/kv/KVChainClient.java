package mmux.kv;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.TException;

public class KVChainClient extends ChainClient {

  KVChainClient(BlockClientCache cache, List<String> chain) throws TException {
    super(cache, chain);
  }

  public ByteBuffer exists(String key) throws TException {
    return runCommand(getTail(), Collections.singletonList(KVOps.EXISTS),
        Collections.singletonList(toArgs(key))).get(0).get(0);
  }

  public ByteBuffer redirectedExists(String key) throws TException {
    return runCommand(getTail(), Collections.singletonList(KVOps.EXISTS),
        Collections.singletonList(toArgs(key, "!redirected"))).get(0).get(0);
  }

  public ByteBuffer get(String key) throws TException {
    return runCommand(getTail(), Collections.singletonList(KVOps.GET),
        Collections.singletonList(toArgs(key))).get(0).get(0);
  }

  public ByteBuffer redirectedGet(String key) throws TException {
    return runCommand(getTail(), Collections.singletonList(KVOps.GET),
        Collections.singletonList(toArgs(key, "!redirected"))).get(0).get(0);
  }

  public ByteBuffer put(String key, String value) throws TException {
    return runCommand(getHead(), Collections.singletonList(KVOps.PUT),
        Collections.singletonList(toArgs(key, value))).get(0).get(0);
  }

  public ByteBuffer redirectedPut(String key, String value) throws TException {
    return runCommand(getHead(), Collections.singletonList(KVOps.PUT),
        Collections.singletonList(toArgs(key, value, "!redirected"))).get(0).get(0);
  }

  public ByteBuffer update(String key, String value) throws TException {
    return runCommand(getHead(), Collections.singletonList(KVOps.UPDATE),
        Collections.singletonList(toArgs(key, value))).get(0).get(0);
  }

  public ByteBuffer redirectedUpdate(String key, String value) throws TException {
    return runCommand(getHead(), Collections.singletonList(KVOps.UPDATE),
        Collections.singletonList(toArgs(key, value, "!redirected"))).get(0).get(0);
  }

  public ByteBuffer remove(String key) throws TException {
    return runCommand(getHead(), Collections.singletonList(KVOps.REMOVE),
        Collections.singletonList(toArgs(key))).get(0).get(0);
  }

  public ByteBuffer redirectedRemove(String key) throws TException {
    return runCommand(getHead(), Collections.singletonList(KVOps.REMOVE),
        Collections.singletonList(toArgs(key, "!redirected"))).get(0).get(0);
  }

  private List<ByteBuffer> toArgs(String... args) {
    List<ByteBuffer> out = new ArrayList<>();
    for (String arg : args) {
      out.add(ByteBuffer.wrap(arg.getBytes(StandardCharsets.UTF_8)));
    }
    return out;
  }
}