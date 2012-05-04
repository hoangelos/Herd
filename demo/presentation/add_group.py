from gevent import monkey
monkey.patch_all()

import time

from gevent import Greenlet
import gevent

from herd.cluster.server import HerdCluster
from herd.cluster.client import HerdClusterClient
from herd.manager.client import HerdClient
from herd.manager.server import HerdManager

#Launch the two managers
def manager1():
    manager = HerdManager(address=None, port=8339, ip='127.0.0.1', config=None,
                          stream_ip='127.0.0.1', stream_port=8338)
    manager.start_listener(forever=False)

def manager2():
    manager = HerdManager(address='127.0.0.1:8339', port=8339, ip='127.0.0.4', config=None,
                          stream_ip='127.0.0.4', stream_port=8338)
    manager.start_listener()

print "Starting Herd Managers"
m1 = Greenlet(manager1)
m1.start()
m2 = Greenlet(manager2)
m2.start()

#Start two cluster nodes
def node1():
    groups=['phalliday']
    managers = ['127.0.0.1:8338']
    seeds = []
    address = '127.0.0.2:14922'
    cluster = HerdCluster(seeds=seeds, address=address, groups=groups, ping_time=5000, managers=managers)
    cluster.start()

def node2():
    groups=['drizzt51','phalliday']
    managers = ['127.0.0.1:8338']
    seeds = []
    address = '127.0.0.3:14922'
    cluster = HerdCluster(seeds=seeds, address=address, groups=groups, ping_time=5000, managers=managers)
    cluster.start()
    
print "Starting Herd Cluster"
g1 = Greenlet(node1)
g1.start()
g2 = Greenlet(node2)
g2.start()

#Start gossip
gevent.sleep(10)
print "Send a Message to drizzt group via CLUSTER NODE #2"
msg = { 'update_local_data': "drizzt51<===>How's the dem going man?!"}
manager_client = HerdClusterClient(seeds=['127.0.0.3'])
response = manager_client.request(msg)

gevent.sleep(8)
print "Verify that CLUSTER NODE #2 has the gossip"
msg2 = { 'get_local_data': 'drizzt51' }
manager_client = HerdClusterClient(seeds=['127.0.0.3'])
response = manager_client.request(msg2)

gevent.sleep(8)
print "Verify that CLUSTER NODE #1 DOESN'T have the gossip"
msg2 = { 'get_local_data': 'drizzt51' }
manager_client = HerdClusterClient(seeds=['127.0.0.2'])
response = manager_client.request(msg2)

gevent.sleep(8)
print "Add CLUSTER NODE #1 to the drizzt group"
msg2 = { 'add_group': 'drizzt51' }
manager_client = HerdClusterClient(seeds=['127.0.0.2'])
response = manager_client.request(msg2)

gevent.sleep(10)
print "Send a Message to phalliday group via CLUSTER NODE #2"
msg = { 'update_local_data': 'drizzt51<===>I bet you are sucking out on this one, punk!'}
manager_client = HerdClusterClient(seeds=['127.0.0.3'])
response = manager_client.request(msg)

gevent.sleep(10)
print "Verify that CLUSTER NODE #1 get the update"
msg2 = { 'get_local_data': 'drizzt51' }
manager_client = HerdClusterClient(seeds=['127.0.0.2'])
response = manager_client.request(msg2)

#Clean up
gevent.killall([
    m1,
    m2,
    g1,
    g2
])
