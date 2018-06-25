package mmux.kv;

import java.util.HashMap;
import mmux.kv.block_request_service.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class BlockClientCache {

  class Key {
    private String host;
    private int port;

    Key(String host, int port) {
      this.host = host;
      this.port = port;
    }

    public int getPort() {
      return port;
    }

    public String getHost() {
      return host;
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

  public BlockClientCache() {
    cache = new HashMap<>();
  }

  public Value get(String host, int port) throws TTransportException {
    Key key = new Key(host, port);
    if (cache.containsKey(key)) {
      return cache.get(key);
    }
    TTransport transport = new TSocket(host, port);
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
    cache.put(key, value);
    return value;
  }
}
