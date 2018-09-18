package mmux.util;

import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.function.Function;
import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ThriftTransportPool {

  private static final int DEFAULT_TIMEOUT = (int) MINUTES.toMillis(5);
  private static final int DEFAULT_MIN_CONNECTIONS = 1;
  private static final int DEFAULT_MAX_CONNECTIONS = 1000;
  private final GenericKeyedObjectPool<ThriftEndpoint, TTransport> cxns;

  public ThriftTransportPool(GenericKeyedObjectPoolConfig config,
      Function<ThriftEndpoint, TTransport> transportProvider) {
    cxns = new GenericKeyedObjectPool<>(new ThriftConnectionFactory(transportProvider), config);
  }

  public ThriftTransportPool(GenericKeyedObjectPoolConfig config, int timeoutMs) {
    this(config, info -> {
      TSocket tsocket = new TSocket(info.getHost(), info.getPort());
      tsocket.setTimeout(timeoutMs);
      return new TFramedTransport(tsocket);
    });
  }

  public ThriftTransportPool(GenericKeyedObjectPoolConfig config) {
    this(config, DEFAULT_TIMEOUT);
  }

  public ThriftTransportPool() {
    this(new GenericKeyedObjectPoolConfig());
  }

  public static ThriftTransportPool getInstance() {
    return LazyHolder.INSTANCE;
  }

  public TTransport getConnection(ThriftEndpoint thriftServerInfo) {
    try {
      return cxns.borrowObject(thriftServerInfo);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** {@inheritDoc} */
  public void returnConnection(ThriftEndpoint thriftServerInfo, TTransport transport) {
    cxns.returnObject(thriftServerInfo, transport);
  }

  /** {@inheritDoc} */
  public void returnBrokenConnection(ThriftEndpoint thriftServerInfo, TTransport transport) {
    try {
      cxns.invalidateObject(thriftServerInfo, transport);
    } catch (Exception ignored) { }
  }

  private static class LazyHolder {
    private static final ThriftTransportPool INSTANCE;
    static {
      GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
      config.setMaxTotal(DEFAULT_MAX_CONNECTIONS);
      config.setMaxTotalPerKey(DEFAULT_MAX_CONNECTIONS);
      config.setMaxIdlePerKey(DEFAULT_MAX_CONNECTIONS);
      config.setMinIdlePerKey(DEFAULT_MIN_CONNECTIONS);
      config.setTestOnBorrow(true);
      config.setMinEvictableIdleTimeMillis(MINUTES.toMillis(1));
      config.setSoftMinEvictableIdleTimeMillis(MINUTES.toMillis(1));
      config.setJmxEnabled(false);
      INSTANCE = new ThriftTransportPool(config);
    }
  }

  public static final class ThriftConnectionFactory implements
      KeyedPooledObjectFactory<ThriftEndpoint, TTransport> {

    private final Function<ThriftEndpoint, TTransport> transportProvider;

    ThriftConnectionFactory(Function<ThriftEndpoint, TTransport> transportProvider) {
      this.transportProvider = transportProvider;
    }

    @Override
    public PooledObject<TTransport> makeObject(ThriftEndpoint endpoint) throws Exception {
      TTransport transport = transportProvider.apply(endpoint);
      transport.open();
      return new DefaultPooledObject<>(transport);
    }

    @Override
    public void destroyObject(ThriftEndpoint endpoint, PooledObject<TTransport> p) {
      TTransport transport = p.getObject();
      if (transport != null && transport.isOpen()) {
        transport.close();
      }
    }

    @Override
    public boolean validateObject(ThriftEndpoint endpoint, PooledObject<TTransport> p) {
      try {
        return p.getObject().isOpen();
      } catch (Throwable e) {
        return false;
      }
    }

    @Override
    public void activateObject(ThriftEndpoint endpoint, PooledObject<TTransport> p) {
      // do nothing
    }

    @Override
    public void passivateObject(ThriftEndpoint endpoint, PooledObject<TTransport> p) {
      // do nothing
    }

  }
}
