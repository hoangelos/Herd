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

print "Starting MANAGER NODE #1"
m1 = Greenlet(manager1)
m1.start()


#Do a ping, to "pre load" a group
gevent.sleep(10)
print "Preload on phalliday group on MASTER NODE #1"
msg = {'command': 'ping', 'data': { 'ip': '127.0.0.2:8338', 'groups': ['phalliday'] } }
manager_client = HerdClient(seed='127.0.0.1:8338')
response = manager_client.request(msg)
print response

#gevent.sleep(10)
#print "See one server at MANAGER NODE #1"
#msg2 = {'command': 'init', 'data': { 'groups': ['phalliday']} }
#manager_client2 = HerdClient(seed='127.0.0.1:8338')
#response = manager_client2.request(msg2)
#print response

m2 = Greenlet(manager2)
m2.start()

gevent.sleep(10)
print "See one server at MANAGER NODE #2"
msg = {'command': 'init', 'data': { 'groups': ['phalliday']} }
manager_client = HerdClient(seed='127.0.0.4:8338')
response = manager_client.request(msg)
print response


print "Close down the MANAGER NODE #2"
gevent.killall([
    m1,
])

#Start two cluster nodes
def node1():
    groups=['phalliday']
    managers = ['127.0.0.1:8338','127.0.0.4:8338']
    seeds = []
    address = '127.0.0.2:14922'
    cluster = HerdCluster(seeds=seeds, address=address, groups=groups, ping_time=5000, managers=managers)
    cluster.start()

def node2():
    groups=['drizzt51','phalliday']
    managers = ['127.0.0.1:8338','127.0.0.4:8338']
    seeds = []
    address = '127.0.0.3:14922'
    cluster = HerdCluster(seeds=seeds, address=address, groups=groups, ping_time=5000, managers=managers)
    cluster.start()
    
print "Starting Cluster Nodes"
g1 = Greenlet(node1)
#g1.start()
g2 = Greenlet(node2)
#g2.start()

gevent.sleep(8)
print "See feedback at MANAGER NODE #1"
msg = {'command': 'init', 'data': { 'groups': ['phalliday']} }
manager_client = HerdClient(seed='127.0.0.4:8338')
response = manager_client.request(msg)
print response



#Clean up
gevent.killall([
    m1,
    g1,
    g2
])
