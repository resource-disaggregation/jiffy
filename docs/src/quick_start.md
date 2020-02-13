# Quick Start

In this Quick Start, we will take a look at how to download and setup Jiffy,
create some data structure to store data, and query it.


## Pre-requisites

Before you can install Jiffy, make sure you have the following prerequisites:

- MacOS X, CentOS, or Ubuntu(16.04 or later)
- C++ compiler that supports C++11 standard (e.g., GCC 5.3 or later)
- CMake 3.9 or later

For Python client, you will additionally require:

- Python 2.7 or later, 3.6 or later
- Python Packages: setuptools

For java client, you will additionally require:

- Java 1.7 or later
- Maven 3.0.4 or later

## Download and Install

To download and install Jiffy, use the following commands:
```bash
git clone https://github.com/ucbrise/jiffy.git
cd jiffy
mkdir -p build
cd build
cmake ..
make -j && make test && make install
```
For macOS Mojave users, the headers are no longer installed under `/usr/include` by default.
Please run the following command in terminal and follow the installation instructions before building:

```bash
open /Library/Developer/CommandLineTools/Packages/macOS_SDK_headers_for_macOS_10.14.pkg
```

## Setup cluster

### Diagram

![Diagram](./img/jiffy_arch.png)



### Start server

After installation, we can first start a directory server by running:

```bash
directoryd --config jiffy.conf
```

We can then start multiple storage servers in different port configurations by running:

```bash
storaged --config jiffy1.conf
storaged --config jiffy2.conf
...
```

We have to start the directory server before the storage server because the storage will find it's corresponding directory server to advertise its blocks right after start up.



### Configuration files

A sample configuration file for Jiffy could be find under jiffy/conf. Users can specify their own configuration parameters to directoryd and storaged executables as follows:

```bash
directoryd --config jiffy.conf

storaged --config jiffy.conf
```





## Using Jiffy

Once the directory server and storage server are running, you can store and query using Python or Java client APIs.



### Client interface


We first create a new client connection to the directory server.

```python tab="Python"
from jiffy import JiffyClient
# Connect the directory server with the corresponding port numbers
client = JiffyClient("127.0.0.1", 9090, 9091)
```

```java tab="Java"
JiffyClient client = JiffyClient("127.0.0.1", 9090, 9091);
```

The first argument to the JiffyClient constructor corresponds to the server hostname, while the second and third argument correspond to the directory server port and lease server port.

We could then create a file and also back it up in persistent storage. The backing path on persistent storage, the number of blocks, the replication chain length and also the flag are the four arguments that can be specified when creating a file. 
The create method also accepts flags as it's last argument, details can be seen in the following chart.

```python tab="Python"
hash_table = client.create_hash_table("/a/file.txt", "local://tmp")

hash_table = client.open_hash_table("/a/file.txt")

hash_table = client.open_or_create_hash_table("/a/file.txt", "local://tmp")
```

```java tab="Java"
client.createHashTable("/a/file.txt", "local://tmp");

HashTableClient kv = client.open("/a/file.txt");

HashTableClient kv = client.openOrCreateHashTable("/a/file.txt", "local://tmp");
```

|   Flags  |      Description     |  Value|
|----------|:--------------------:|------:|
| PINNED |  If set, pins memory in Jiffy, and does not free it when lease expires | 0x01 |
| STATIC_PROVISIONED |    If set, does not perform auto scaling // NO LONGER USED   |   0x02 |
| MAPPED | Map the data structure to a persistent store |    0x04 |


The open and create operations will return a data structure client, from which we could achieve specific operations on the file data.



The Jiffy client could also sychronize its data with the persistent storage.The operations are as follows:

```python tab="Python"
# Synchronize file data with persistent storage
client.sync("/a/file.txt", "local://tmp")

# Write dirty file back to persistent storage and clear the file
client.dump("/a/file.txt", "local://tmp")

# Load file from persistent storage
client.load("/a/file.txt", "local://tmp")
```

```java tab="Java"
client.sync("/a/file.txt", "local://tmp");

client.dump("/a/file.txt", "local://tmp");

client.load("/a/file.txt", "local://tmp");
```



The Jiffy client could also listen after a path on a specific operation and receive notification via the subscription client.

```python tab="Python"

client = JiffyClient("127.0.0.1", 9090, 9091)
client.create_hash_table("/a/file.txt", "local://tmp")
# Create subscription client listenning to specific file
subscription = client.listen("/a/file.txt")
# Subscription client listenning on operation 'put'
subscription.subscribe(['put'])

hash_table = client.open_hash_table("/a/file.txt")
hash_table.put(b'key1', b'value1')
hash_table.remove(b'key1')

# Receive notification
# The expected output would be {op=put, data=b'key1'}
print(subscription.get_notification())

# Unsubscribe operation
subscription.unsubscrbe(['put'])

# Disconnect
subscription.disconnect()

client.close("/a/file.txt")
client.remove("/a/file.txt")
client.disconnect()

```

```java tab="Java"
JiffyClient client = JiffyClient("127.0.0.1", 9090, 9091);
String op1 = "put";
ByteBuffer key = ByteBufferUtils.fromString("key1");
ByteBuffer value = ByteBufferUtils.fromString("value1");
client.createHashTable("/a/file.txt", "local://tmp", 1, 1);
// Create listenner for specific file
KVListener n1 = client.listen("/a/file.txt");
// Subscribe for a operation
n1.subscribe(Collections.singletonList(op1));

KVClient kv = client.open("/a/file.txt");
kv.put(key, value);
kv.remove(key);

// Receive notification
Notification N1 = n1.getNotification();

// Unsubscribe operation
n1.unsubscribe(Collections.singletonList(op1));

client.close("/a/file.txt");
client.remove("/a/file.txt");
client.close();
```






After operating on the file, the client could also close or remove the file and disconnect the server.

```python tab="Python"
client.close("/a/file.txt")
client.remove("/a/file.txt")
client.disconnect()
```

```java tab="Java"
client.close("/a/file.txt");
client.remove("/a/file.txt");
client.close();
```



### Hash table API 

After file creation, user can achieve multiple commands with the hash table client.



We can put key-value pairs in the storage server and also get them in byte-strings.

```python tab="Python"
hash_table.put(b"key", b"value")
if hash_table.exists(b"key") == b'true':
	value = hash_table.get(b"key")
```

```java tab="Java"
ByteBuffer key = ByteBufferUtils.fromString("key1");
ByteBuffer value = ByteBufferUtils.fromString("value1");
KVClient kv = client.open("/a/file.txt");
kv.put(key, value);
if(kv.exists(key))
    value2 = kv.get(key);
```



We can also update or remove specific key-value pairs as follows:

```python tab="Python"
hash_table.update(b"key", b"value")
hash_table.remove(b"key")
```

```java tab="Java"
kv.update(key, value);
kv.remove(key);
```



All the operation can be done in batches as follows:

```python tab="Python"
args = [b"key1", b"value1", b"key2", b"value2"]
args_key = [b"key1", b"key2", b"key3"]
# Put in batches
response = hash_table.multi_put(args)
# Get in batches
response = hash_table.multi_get(args_key)
# Check exists in batches
response = hash_table.multi_exists(args_key)
# Update in batches
response = hash_table.multi_update(args)
# Remove in batches
response = hash_table.multi_remove(args_key)

```

```java tab="Java"
ByteBuffer key1 = ByteBufferUtils.fromString("key1");
ByteBuffer value1 = ByteBufferUtils.fromString("value1");
ByteBuffer key2 = ByteBufferUtils.fromString("key2");
ByteBuffer value2 = ByteBufferUtils.fromString("value2");
List<ByteBuffer> args = Arrays.asList(key1, value1, key2, value2);
List<ByteBuffer> args_key = Arrays.asList(key1, key2);
// Put in batches
response = kv.put(args);
// Get in batches
response = kv.get(args_key);
// Check exists in batches
response = kv.exists(args_key);
// Update in batches
response = kv.update(args);
// Remove in batches
response = kv.remove(args_key);

```

