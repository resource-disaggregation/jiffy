package mmux.util;

import com.github.phantomthief.thrift.client.ThriftClient;
import com.github.phantomthief.thrift.client.impl.ThriftClientImpl;
import com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider;
import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import com.github.phantomthief.thrift.client.pool.impl.DefaultThriftConnectionPoolImpl;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ThriftPool {
  public static ThriftClient clientPool(String host, int port) {
    Function<ThriftServerInfo, TTransport> tProvider = info -> {
      TSocket socket = new TSocket(info.getHost(), info.getPort());
      return new TFramedTransport(socket);
    };
    GenericKeyedObjectPoolConfig pConf = new GenericKeyedObjectPoolConfig();
    ThriftServerInfo info = ThriftServerInfo.of(host, port);
    Supplier<List<ThriftServerInfo>> iProvider = () -> Collections.singletonList(info);
    ThriftConnectionPoolProvider pProvider = new DefaultThriftConnectionPoolImpl(pConf, tProvider);
    return new ThriftClientImpl(iProvider, pProvider);
  }
}
