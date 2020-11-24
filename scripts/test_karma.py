import time
import os
import sys
from jiffy import JiffyClient

client = JiffyClient("127.0.0.1", 9090, 9091)
client.fs.add_tags('advertise_demand', {'tenant_id': 'a', 'demand': '10'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'b', 'demand': '10'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'c', 'demand': '10'})

time.sleep(5)

client.fs.add_tags('advertise_demand', {'tenant_id': 'a', 'demand': '2'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'b', 'demand': '12'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'c', 'demand': '16'})

time.sleep(5)

client.fs.add_tags('advertise_demand', {'tenant_id': 'a', 'demand': '6'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'b', 'demand': '12'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'c', 'demand': '6'})

time.sleep(5)

client.fs.add_tags('advertise_demand', {'tenant_id': 'a', 'demand': '18'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'b', 'demand': '5'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'c', 'demand': '12'})

time.sleep(5)

client.fs.add_tags('advertise_demand', {'tenant_id': 'a', 'demand': '9'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'b', 'demand': '13'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'c', 'demand': '8'})

time.sleep(5)

client.fs.add_tags('advertise_demand', {'tenant_id': 'a', 'demand': '10'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'b', 'demand': '10'})
client.fs.add_tags('advertise_demand', {'tenant_id': 'c', 'demand': '10'})
