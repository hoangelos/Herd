import logging
import random

from herd.cluster.server import HerdCluster
from herd.manager.client import HerdClient

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

groups=['phalliday']
managers = ['127.0.0.1:8338']
msg = { 'command': 'init', 'data': { 'groups': groups } }
seeds = []
address = '127.0.0.2:14922'

client = HerdClient(seed=random.choice(managers))
response = client.request(msg)
if response and len(response['data']['groups']):
    for item in response['data']['groups']:        
        for value in item.values():
            for ip in value:
                if ip not in seeds and address != ip:
                    seeds.append(ip)


cluster = HerdCluster(seeds=seeds, address=address, groups=groups, ping_time=5000, managers=managers)
cluster.start()