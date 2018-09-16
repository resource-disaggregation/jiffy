/**
 * 
 */
package com.github.phantomthief.thrift.client.pool;

import org.apache.thrift.transport.TTransport;

/**
 * <p>
 * ThriftConnectionPoolProvider interface.
 * </p>
 *
 * @author w.vela
 * @version $Id: $Id
 */
public interface ThriftConnectionPoolProvider {

    /**
     * <p>
     * getConnection.
     * </p>
     *
     * @param thriftServerInfo a
     *        {@link com.github.phantomthief.thrift.client.pool.ThriftServerInfo} object.
     * @return a {@link org.apache.thrift.transport.TTransport} object.
     */
    TTransport getConnection(ThriftServerInfo thriftServerInfo);

    /**
     * <p>
     * returnConnection.
     * </p>
     *
     * @param thriftServerInfo a
     *        {@link com.github.phantomthief.thrift.client.pool.ThriftServerInfo} object.
     * @param transport a {@link org.apache.thrift.transport.TTransport}
     *        object.
     */
    void returnConnection(ThriftServerInfo thriftServerInfo, TTransport transport);

    /**
     * <p>
     * returnBrokenConnection.
     * </p>
     *
     * @param thriftServerInfo a
     *        {@link com.github.phantomthief.thrift.client.pool.ThriftServerInfo} object.
     * @param transport a {@link org.apache.thrift.transport.TTransport}
     *        object.
     */
    void returnBrokenConnection(ThriftServerInfo thriftServerInfo, TTransport transport);

}
