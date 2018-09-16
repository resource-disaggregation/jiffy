thrift-pool-client
=======================

A Thrift Client pool for Java

* raw and type safe TServiceClient pool
* Multi backend servers support
* Backend servers replace on the fly
* Backend route by hash or random
* Failover and failback support
* jdk 1.8 only

## Get Started

```xml
<dependency>
    <groupId>com.github.phantomthief</groupId>
    <artifactId>thrift-pool-client</artifactId>
    <version>1.0.0</version>
</dependency>
```

```Java

// init a thrift client
ThriftClient thriftClient = new ThriftClientImpl(() -> Arrays.asList(//
        ThriftServerInfo.of("127.0.0.1", 9090), //
        ThriftServerInfo.of("127.0.0.1", 9091) //
        // or you can return a dynamic result.
        ));
// get iface and call
System.out.println(thriftClient.iface(Client.class).echo("hello world."));

// get iface and call to Multiplexed Server
System.out.println(thriftClient.mpiface(Client.class, "ClientService").echo("hello world."));


// get iface with custom hash, the same hash return the same thrift backend server
System.out.println(thriftClient.iface(Client.class, "hello world".hashCode()).echo(
        "hello world"));

// get mpiface with custom hash, the same hash return the same thrift backend server
System.out.println(thriftClient.iface(Client.class, "ClientService", "hello world".hashCode()).echo(
        "hello world"));


// customize protocol
System.out.println(thriftClient.iface(Client.class, TBinaryProtocol::new,
        "hello world".hashCode()).echo("hello world"));

// customize protocol to Multiplexed Server
System.out.println(thriftClient.mpiface(Client.class, "ClientService", TBinaryProtocol::new,
        "hello world".hashCode()).echo("hello world"));


// customize pool config
GenericKeyedObjectPoolConfig poolConfig = new GenericKeyedObjectPoolConfig();
// ... customize pool config here


// customize transport, while if you expect pooling the connection, you should use TFrameTransport.
Function<ThriftServerInfo, TTransport> transportProvider = info -> {
    TSocket socket = new TSocket(info.getHost(), info.getPort());
    TFramedTransport transport = new TFramedTransport(socket);
    return transport;
};
ThriftClient customizeThriftClient = new ThriftClientImpl(() -> Arrays.asList(//
        ThriftServerInfo.of("127.0.0.1", 9090), //
        ThriftServerInfo.of("127.0.0.1", 9091) //
        ), new DefaultThriftConnectionPoolImpl(poolConfig, transportProvider));
customizeThriftClient.iface(Client.class).echo("hello world.");


// init a failover thrift client
ThriftClient failoverThriftClient = new FailoverThriftClientImpl(() -> Arrays.asList(//
        ThriftServerInfo.of("127.0.0.1", 9090), //
        ThriftServerInfo.of("127.0.0.1", 9091) //
        ));
failoverThriftClient.iface(Client.class).echo("hello world.");


// a customize failover client, if the call fail 10 times in 30 seconds, the backend server will be marked as fail for 1 minutes.
FailoverCheckingStrategy<ThriftServerInfo> failoverCheckingStrategy = new FailoverCheckingStrategy<>(
        10, TimeUnit.SECONDS.toMillis(30), TimeUnit.MINUTES.toMillis(1));
ThriftClient customizedFailoverThriftClient = new FailoverThriftClientImpl(
        failoverCheckingStrategy, () -> Arrays.asList(//
                ThriftServerInfo.of("127.0.0.1", 9090), //
                ThriftServerInfo.of("127.0.0.1", 9091) //
                ), DefaultThriftConnectionPoolImpl.getInstance());
customizedFailoverThriftClient.iface(Client.class).echo("hello world.");
    
```

## Know issues

You shouldn't reuse iface returned by client.

## Special Thanks

perlmonk with his great team gives me a huge help.
(https://github.com/aloha-app/thrift-client-pool-java)
