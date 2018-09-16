/**
 * 
 */
package com.github.phantomthief.thrift.client.impl;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import com.github.phantomthief.thrift.client.ThriftClient;
import com.github.phantomthief.thrift.client.exception.NoBackendException;
import com.github.phantomthief.thrift.client.pool.ThriftConnectionPoolProvider;
import com.github.phantomthief.thrift.client.pool.ThriftServerInfo;
import com.github.phantomthief.thrift.client.pool.impl.DefaultThriftConnectionPoolImpl;
import com.github.phantomthief.thrift.client.utils.ThriftClientUtils;

import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

/**
 * <p>
 * ThriftClientImpl class.
 * </p>
 *
 * @author w.vela
 * @version $Id: $Id
 */
public class ThriftClientImpl implements ThriftClient {

    private final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(getClass());

    private final ThriftConnectionPoolProvider poolProvider;

    private final Supplier<List<ThriftServerInfo>> serverInfoProvider;

    /**
     * <p>
     * Constructor for ThriftClientImpl.
     * </p>
     *
     * @param serverInfoProvider provide service list
     */
    public ThriftClientImpl(Supplier<List<ThriftServerInfo>> serverInfoProvider) {
        this(serverInfoProvider, DefaultThriftConnectionPoolImpl.getInstance());
    }

    /**
     * <p>
     * Constructor for ThriftClientImpl.
     * </p>
     *
     * @param serverInfoProvider provide service list
     * @param poolProvider provide a pool
     */
    public ThriftClientImpl(Supplier<List<ThriftServerInfo>> serverInfoProvider,
            ThriftConnectionPoolProvider poolProvider) {
        this.poolProvider = poolProvider;
        this.serverInfoProvider = serverInfoProvider;
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * iface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass) {
        return iface(ifaceClass, ThriftClientUtils.randomNextInt());
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * iface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass, int hash) {
        return iface(ifaceClass, TCompactProtocol::new, hash);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * iface.
     * </p>
     */
    @SuppressWarnings("unchecked")
    @Override
    public <X extends TServiceClient> X iface(Class<X> ifaceClass,
            Function<TTransport, TProtocol> protocolProvider, int hash) {
        List<ThriftServerInfo> servers = serverInfoProvider.get();
        if (servers == null || servers.isEmpty()) {
            throw new NoBackendException();
        }
        hash = Math.abs(hash);
        hash = hash < 0 ? 0 : hash;
        ThriftServerInfo selected = servers.get(hash % servers.size());
        logger.trace("get connection for [{}]->{} with hash:{}", ifaceClass, selected, hash);

        TTransport transport = poolProvider.getConnection(selected);
        TProtocol protocol = protocolProvider.apply(transport);

        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(ifaceClass);
        factory.setFilter(m -> ThriftClientUtils.getInterfaceMethodNames(ifaceClass).contains(
                m.getName()));
        try {
            X x = (X) factory.create(new Class[] { org.apache.thrift.protocol.TProtocol.class },
                    new Object[] { protocol });
            ((Proxy) x).setHandler((self, thisMethod, proceed, args) -> {
                boolean success = false;
                try {
                    Object result = proceed.invoke(self, args);
                    success = true;
                    return result;
                } finally {
                    if (success) {
                        poolProvider.returnConnection(selected, transport);
                    } else {
                        poolProvider.returnBrokenConnection(selected, transport);
                    }
                }
            });
            return x;
        } catch (NoSuchMethodException | IllegalArgumentException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("fail to create proxy.", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * mpiface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName) {
        return mpiface(ifaceClass, serviceName, ThriftClientUtils.randomNextInt());
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * mpiface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName, int hash) {
        return mpiface(ifaceClass, serviceName, TCompactProtocol::new, hash);
    }

    /**
     * {@inheritDoc}
     *
     * <p>
     * mpiface.
     * </p>
     */
    @Override
    public <X extends TServiceClient> X mpiface(Class<X> ifaceClass, String serviceName,
        Function<TTransport, TProtocol> protocolProvider, int hash) {
        return iface(ifaceClass,
            protocolProvider.andThen((p) -> new TMultiplexedProtocol(p, serviceName)), hash);
    }

}
