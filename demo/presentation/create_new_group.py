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

print "Ensure there are no existing groups in the cluster"
gevent.sleep(8)
msg = {'command': 'groups', 'data': None }
manager_client = HerdClient(seed='127.0.0.1:8338')
response = manager_client.request(msg)
print response

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
print "Ensure there are two existing groups in the cluster"
msg = {'command': 'groups', 'data': None }
manager_client = HerdClient(seed='127.0.0.1:8338')
response = manager_client.request(msg)
print response

#Clean up
gevent.killall([
    m1,
    m2,
    g1,
    g2
])