from jiffy import JiffyClient

client = JiffyClient("127.0.0.1", 9090, 9091)

kv = client.open("/a/file.txt")
print(kv.get(b"key"))
client.disconnect

