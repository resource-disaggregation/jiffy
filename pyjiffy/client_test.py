from jiffy import JiffyClient

client = JiffyClient("127.0.0.1", 9090, 9091);

client.create("/a/file.txt", "local://tmp")
kv = client.open("/a/file.txt")
#args = [b"key1"]
#args.append(b"value1")
#args.append(b"key2")
#args.append(b"value2")
args = [b"key1", b"value1", b"key2", b"value2"]
#kv.put(b"key", b"value")
kv.multi_put(args)
print(kv.exists(b"key2"))

#subscription = client.listen("/a/file.txt")
#subscription.subscribe("get")
#while not subscription.has_notification():
#    notify = subscription.get_notification()
#    print(notify)
#print (kv)
#print(kv.get(b"key"))
client.close("/a/file.txt")
client.remove("/a/file.txt")
client.disconnect()
