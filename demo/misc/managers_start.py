import time

from gevent import Greenlet
import gevent

from herd.cluster.server import HerdCluster
from herd.manager.client import HerdClient
from herd.manager.server import HerdManager

def manager1():
    manager = HerdManager(address=None, port=8339, ip='127.0.0.1', config=None,
                          stream_ip='127.0.0.1', stream_port=8338)
    manager.start_listener(forever=True)

def manager2():
    manager = HerdManager(address='127.0.0.1:8339', port=8339, ip='127.0.0.4', config=None,
                          stream_ip='127.0.0.4', stream_port=8338)
    manager.start_listener()

gevent.joinall([
    gevent.spawn(manager1),
    #gevent.spawn(manager2)
])

time.sleep(5);
msg = {'command': 'groups', 'data': None }
manager_client = HerdClient(seed='127.0.0.1:8338')
response = manager_client.request(msg)
print response