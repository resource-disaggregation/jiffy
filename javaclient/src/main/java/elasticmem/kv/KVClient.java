package elasticmem.kv;

import elasticmem.directory.rpc_data_status;
import java.io.Closeable;
import java.nio.ByteBuffer;
import org.apache.thrift.TException;

public class KVClient implements Closeable {

  private KVChainClient[] clients;

  class PipelinedRequestHandle {

    private KVChainClient client;
    private long opSeq;

    PipelinedRequestHandle(KVChainClient client, long opSeq) {
      this.client = client;
      this.opSeq = opSeq;
    }

    public ByteBuffer get() throws TException {
      return client.receiveResponse(opSeq);
    }
  }

  public KVClient(rpc_data_status dataStatus) throws TException {
    this.clients = new KVChainClient[dataStatus.data_blocks.size()];
    for (int i = 0; i < clients.length; i++) {
      clients[i] = new KVChainClient(dataStatus.data_blocks.get(i).block_names);
    }
  }

  @Override
  public void close() {
    for (KVChainClient client : clients) {
      client.close();
    }
  }

  public ByteBuffer get(ByteBuffer key) throws TException {
    return clients[key.hashCode() % clients.length].get(key);
  }

  public PipelinedRequestHandle pipelineGet(ByteBuffer key) throws TException {
    KVChainClient client = clients[key.hashCode() % clients.length];
    return new PipelinedRequestHandle(client, client.pipelineGet(key));
  }

  public ByteBuffer put(ByteBuffer key, ByteBuffer value) throws TException {
    return clients[key.hashCode() % clients.length].put(key, value);
  }

  public PipelinedRequestHandle pipelinePut(ByteBuffer key, ByteBuffer value) throws TException {
    KVChainClient client = clients[key.hashCode() % clients.length];
    return new PipelinedRequestHandle(client, client.pipelinePut(key, value));
  }

  public ByteBuffer update(ByteBuffer key, ByteBuffer value) throws TException {
    return clients[key.hashCode() % clients.length].update(key, value);
  }

  public PipelinedRequestHandle pipelineUpdate(ByteBuffer key, ByteBuffer value) throws TException {
    KVChainClient client = clients[key.hashCode() % clients.length];
    return new PipelinedRequestHandle(client, client.pipelineUpdate(key, value));
  }

  public ByteBuffer remove(ByteBuffer key) throws TException {
    return clients[key.hashCode() % clients.length].remove(key);
  }

  public PipelinedRequestHandle pipelineRemove(ByteBuffer key) throws TException {
    KVChainClient client = clients[key.hashCode() % clients.length];
    return new PipelinedRequestHandle(client, client.pipelineRemove(key));
  }

  public long numKeys() throws TException {
    long nKeys = 0;
    for (KVChainClient client: clients) {
      nKeys += client.numKeys();
    }
    return nKeys;
  }

}
