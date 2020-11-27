import time
from jiffy import JiffyClient
client = JiffyClient("127.0.0.1", 9090, 9091)

client.create_file('/a/foo.txt', 'local://tmp')
client.create_file('/a/foo1.txt', 'local://tmp')
client.create_file('/a/foo2.txt', 'local://tmp')
client.create_file('/a/foo3.txt', 'local://tmp')

foo = client.open_file('/a/foo.txt')
foo1 = client.open_file('/a/foo1.txt')
foo2 = client.open_file('/a/foo2.txt')
foo3 = client.open_file('/a/foo3.txt')

foo.write(b'hello')
foo1.write(b'hello')
foo2.write(b'hello')
foo3.write(b'hello')

client.create_file('/b/bar.txt', 'local://tmp')

while True:
    time.sleep(5)
