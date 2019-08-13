package jiffy.storage;

import java.util.HashMap;
import jiffy.storage.block_request_service.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BlockClientCache {

  class Key {
    private String host;
    private int port;
    private String endpoint;

    Key(String host, int port) {
      this.host = host;
      this.port = port;
      this.endpoint = host + ":" + port;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
    }

    @Override
    public boolean equals(Object o) {
      if (o == this)
        return true;

      if (!(o instanceof Key))
        return false;

      Key k = (Key) o;
      return host.equals(k.host) && port == k.port;
    }

    @Override
    public int hashCode() {
      return endpoint.hashCode();
    }
  }

  class Value {
    private TTransport transport;
    private TProtocol protocol;
    private Client client;

    Value(TTransport transport, TProtocol protocol, Client client) {
      this.transport = transport;
      this.protocol = protocol;
      this.client = client;
    }

    public TTransport getTransport() {
      return transport;
    }

    public TProtocol getProtocol() {
      return protocol;
    }

    public Client getClient() {
      return client;
    }
  }

  private HashMap<Key, Value> cache;
  private int timeoutMs;

  BlockClientCache(int timeoutMs) {
    this.cache = new HashMap<>();
    this.timeoutMs = timeoutMs;
  }

  public Value get(String host, int port) throws TTransportException {
//    Key key = new Key(host, port);
//    if (cache.containsKey(key)) {
//      return cache.get(key);
//    }
    TSocket socket = new TSocket(host, port);
    socket.setTimeout(timeoutMs);
    TFramedTransport transport = new TFramedTransport(socket);
    TProtocol protocol = new TBinaryProtocol(transport);
    Client client = new Client(protocol);
    TTransportException ex = null;
    int attemptsLeft = 3;
    while (attemptsLeft > 0) {
      try {
        transport.open();
        break;
      } catch (TTransportException e) {
        ex = e;
        attemptsLeft--;
      }
    }
    if (attemptsLeft == 0) {
      throw ex;
    }
    Value value = new Value(transport, protocol, client);
    // cache.put(key, value);
    return value;
  }

  public void remove(String host, int port) {
//    Key k = new Key(host, port);
//    if (cache.containsKey(k)) {
//      Value value = cache.remove(k);
//      if (value.getTransport().isOpen()) {
//        value.getTransport().close();
//      }
//    }
  }
}
