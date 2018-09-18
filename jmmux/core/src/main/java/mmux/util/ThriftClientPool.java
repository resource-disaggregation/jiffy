package mmux.util;

import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Supplier;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

public class ThriftClientPool {

  private final ThriftTransportPool transportPool;
  private final Supplier<ThriftEndpoint> endpointSupplier;
  private static ConcurrentMap<Class<?>, Set<String>> interfaceMethodCache = new ConcurrentHashMap<>();

  public ThriftClientPool(Supplier<ThriftEndpoint> endpointSupplier) {
    this(endpointSupplier, ThriftTransportPool.getInstance());
  }

  public ThriftClientPool(Supplier<ThriftEndpoint> endpointSupplier,
      ThriftTransportPool transportPool) {
    this.endpointSupplier = endpointSupplier;
    this.transportPool = transportPool;
  }

  public static ThriftClientPool build(String host, int port) {
    return new ThriftClientPool(() -> ThriftEndpoint.make(host, port), new ThriftTransportPool());
  }

  public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
    return iface(ifaceClass, TBinaryProtocol::new);
  }

  private static Set<String> getInterfaceMethodNames(Class<?> ifaceClass) {
    return interfaceMethodCache.computeIfAbsent(ifaceClass, i -> of(i.getInterfaces()) //
        .flatMap(c -> of(c.getMethods())) //
        .map(Method::getName) //
        .collect(toSet()));
  }

  @SuppressWarnings("unchecked")
  public <X extends TServiceClient> X iface(Class<X> ifaceClass,
      Function<TTransport, TProtocol> protocolProvider) {
    ThriftEndpoint selected = endpointSupplier.get();
    if (selected == null) {
      throw new NoSuchElementException("No backing endpoint");
    }

    TTransport transport = transportPool.getConnection(selected);
    TProtocol protocol = protocolProvider.apply(transport);

    ProxyFactory factory = new ProxyFactory();
    factory.setSuperclass(ifaceClass);
    factory.setFilter(m -> getInterfaceMethodNames(ifaceClass).contains(m.getName()));
    try {
      X x = (X) factory.create(new Class[]{TProtocol.class}, new Object[]{protocol});
      ((Proxy) x).setHandler((self, thisMethod, proceed, args) -> {
        boolean success = false;
        try {
          Object result = proceed.invoke(self, args);
          success = true;
          return result;
        } finally {
          if (success) {
            transportPool.returnConnection(selected, transport);
          } else {
            transportPool.returnBrokenConnection(selected, transport);
          }
        }
      });
      return x;
    } catch (NoSuchMethodException | IllegalArgumentException | InstantiationException
        | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("Failed to create proxy", e);
    }
  }
}
