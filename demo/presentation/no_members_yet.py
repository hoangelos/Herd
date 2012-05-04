from gevent import monkey
monkey.patch_all()

import time

from gevent import Greenlet
import gevent

from herd.cluster.server import HerdCluster
from herd.manager.client import HerdClient
from herd.manager.server import HerdManager
from herd.cluster.client import HerdClusterClient

#Launch the two managers
def manager1():
    manager = HerdManager(address=None, port=8339, ip='127.0.0.1', config=None,
                          stream_ip='127.0.0.1', stream_port=8338)
    manager.start_listener(forever=False)

def manager2():
    manager = HerdManager(address='127.0.0.1:8339', port=8339, ip='127.0.0.4', config=None,
                          stream_ip='127.0.0.4', stream_port=8338)
    manager.start_listener(forever=False)


print "Starting Herd Managers"
m1 = Greenlet(manager1)
m1.start()
m2 = Greenlet(manager2)
m2.start()

#Do a ping, to "pre load" a group
gevent.sleep(10)
print "Preload on phalliday group on MASTER NODE #2"
msg = {'command': 'ping', 'data': { 'ip': '127.0.0.8:8338', 'groups': ['phalliday'] } }
manager_client = HerdClient(seed='127.0.0.4:8338')
response = manager_client.request(msg)

#Show that the group exists
print "Verify group phalliday was created by asking MASTER NODE #1"
msg = {'command': 'groups', 'data': None }
manager_client = HerdClient(seed='127.0.0.1:8338')
response = manager_client.request(msg)
print response

#Start one cluster nodes, from the group above
def node1():
    groups=['phalliday']
    managers = ['127.0.0.1:8338']
    seeds = []
    address = '127.0.0.2:14922'
    cluster = HerdCluster(seeds=seeds, address=address, groups=groups, ping_time=5000, managers=managers)
    cluster.start()
    
print "Starting Herd Cluster"
g1 = Greenlet(node1)
g1.start()

#Show that there's no one else in the seeds list.
gevent.sleep(8)
print "Get the list of peers from CLUSTER NODE #1"
msg = {'get_seeds': None }
manager_client = HerdClusterClient(seeds=['127.0.0.2'])
response = manager_client.request(msg)

#Show that the server shows the new IP as a part of the group.
print "Get the list of cluster servers in phalliday group from MASTER NODE #2"
msg = {'command': 'groups', 'data': None }
manager_client = HerdClient(seed='127.0.0.1:8338')
response = manager_client.request(msg)
print response

#Clean up
gevent.killall([
    m1,
    m2,
    g1
])
