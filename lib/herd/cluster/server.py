# -*- coding: utf-8 -*-
## Copyright 2012 Peter Halliday
##
##   Licensed under the Apache License, Version 2.0 (the "License");
##   you may not use this file except in compliance with the License.
##   You may obtain a copy of the License at
##
##       http://www.apache.org/licenses/LICENSE-2.0
##
##   Unless required by applicable law or agreed to in writing, software
##   distributed under the License is distributed on an "AS IS" BASIS,
##   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
##   See the License for the specific language governing permissions and
##   limitations under the License.

import json
import logging
import random

from elastica.services import BaseService
from config import Config
import daemon
from tornado import ioloop
from tornado import netutil
from tornado.iostream import IOStream

from herd.cluster.monitor import MessagingMonitor
from herd.cluster.listener import MessageListener
from herd.manager.client import HerdClient
from herd import herd_config

class HerdCluster(BaseService):
    """The cluster nodes, that do the actual sharing of messages"""
    def __init__(self, seeds=[], address=None, groups=[], ping_time=None, managers=[], config=None):
        self.logger = logging.getLogger('herd.cluster.server')
        self.seeds = seeds
        self.address = address
        self._groups = groups
        self.ping_time = ping_time
        self.config = config
        self.managers = managers
        self._local_data = {}
        self._load_config()
        self._publishers = {}
        super(HerdCluster, self).__init__(seeds=self.seeds, address=self.address)
        listener = MessageListener(change_callback=self.update_local_data)
        self._gossiper.register_node_state_change_listeners(listener)
        for group in self._groups:
            self._publishers[group] = MessagingMonitor(group=group, group_data_cb=self.get_local_data)
            self._gossiper.register_application_state_publisher(self._publishers[group])
        self._gossiper.register_background_service(self._ping_managers, self.ping_time)
        self._gossiper.register_background_service(self._update_seeds, self.ping_time)
        self.start_listener()
    
    def _load_config(self):
        """Load up the config for the cluster"""
        self.logger.info('loading config for the cluster. (%s)' % self.address)
        # Open the file at default lcoation, unless something else
        # is passed in instead
        if self.config is not None:
            self.logger.debug("Loading up a file for the config")
            f = file(self.config)
            self.cfg = Config(f)
        
            # Allow parameters passed on the command line to override the
            # config file
            if self.address is None:
                self.logger.debug("The address is being loaded from a config file.")
                self.address = self.cfg.cluster.address
            if self.seeds == []:
                self.logger.debug("The seeds are being loaded from a config file.")
                self.seeds = list(self.cfg.cluster.seeds)
            if self._groups == []:
                self.logger.debug("The groups are being loaded from a config file.")
                self._groups = list(self.cfg.cluster.groups)
            if self.ping_time == None:
                self.logger.debug("The ping_time is being loaded from a config file.")
                self.ping_time = self.cfg.cluster.ping_time
            if self.managers == []:
                self.logger.debug("The managers are being loaded from a config file.")
                self.managers = list(self.cfg.cluster.managers)
    
    def start_listener(self):
        """Start the control port."""
        self.logger.info("Start control port (%s:%d)" % (self._host, 8888))
        (self._sock,) = netutil.bind_sockets(8888, address=self._host)
        netutil.add_accept_handler(self._sock, self._accept_coonection)
    
    def _accept_coonection(self, connection, address):
        """When a connection comes in accept it and setup a thread to process it."""
        (host, port) = address
        self.logger.info("Accepting connections from (%s:%s)" % (host, port))
        self._control_stream = IOStream(connection)
        self._control_stream.read_until('\n\n',self._read_from_socket)
    
    def _read_from_socket(self, data):
        """Read the messages sent and process them."""
        self.logger.info("Reading from socket data...")
        obj = json.loads(data)
        key = None
        response = None
        if obj.has_key('add_group'):
            self.logger.info("Processing ADD_GROUP message")
            key = 'add_group'
            response = self.add_group(obj['add_group'])
        elif obj.has_key('remove_group'):
            self.logger.info("Processing REMOVE_GROUP message")
            key = 'remove_group'
            response = self.remove_group(obj['remove_group'])
        elif obj.has_key('get_local_data'):
            self.logger.info("Processing GET_LOCAL_DATA message")
            key = 'get_local_data'
            response = self.get_local_data(obj['get_local_data'])
        elif obj.has_key('update_local_data'):
            self.logger.info("Processing UPDATE_LOCAL_DATA message")
            key = 'update_local_data'
            msg = obj['update_local_data']
            (group,value) = msg.split('<===>')
            response = self.update_local_data(group, value)
        elif obj.has_key('get_seeds'):
            self.logger.info("Processing GET_SEEDS message")
            key = 'get_seeds'
            response = self.seeds
        if not self._control_stream.closed():
            self.logger.debug("Writing message back.")
            msg_obj = { key: response }
            msg = json.dumps(msg_obj)
            self._control_stream.write(msg+"\n\n")
    
    def get_local_data(self, group=None):
        """Get the local data about a particular group."""
        self.logger.info('getting the local data for the cluster (%s)' % self.address)
        data = None
        if group in self._groups:
            try:
                data = self._local_data[group]
            except KeyError, e:
                self.logger.debug("The group (%s) has no messages yet" % group )
        return data
    
    def update_local_data(self, group, value):
        """Update the local data. Any node can write, there is no master."""
        self.logger.info('Updating messages for group (%s)' % group)
        if group in self._groups:
            self._local_data[group] = value
        return value == self._local_data[group]
    
    def add_group(self, group):
        """Call to add this node to a group."""
        self.logger.info('Adding a group to the cluster (%s)' % self.address)
        self._groups.append(group)
        self._publishers[group] = MessagingMonitor(group=group, group_data_cb=self.get_local_data)
        self._gossiper.register_application_state_publisher(self._publishers[group])
        return group in self._groups
    
    def remove_group(self, group):
        """Call to remove this node from the group."""
        self.logger.info('Removing a group from the cluster (%s)' % self.address)
        if group not in self._groups:
            self.logger.debug("Can't remove group (%s), because it doesn't exist as a group" % group)
            return False
        self._gossiper._application_state_publishers.remove(self._publishers[group])
        del self._publishers[group]
        self._groups.remove(group)
        return group not in self._groups
    
    def _ping_managers(self):
        """A background process to let the manager nodes know we are still in this group."""
        self.logger.info('Pinging the manager from (%s)' % self.address)
        sent_to = None
        if self.managers:
            self.logger.debug("There were managers listed, so prep the message to send.")
            manager = random.choice(self.managers)
            msg = { 'command': 'ping', 'data': { 'ip': self.address, 'groups': self._groups } }
            manager_send = HerdClient(seed=manager)
            response = manager_send.request(msg)
            sent_to = manager
        return sent_to
    
    def _update_seeds(self):
        """A background process to get the current list of active nodes for this group."""
        self.logger.info('Getting new batch of seeds from the manager (%s)' % self.address)
        sent_to = None
        seeds = []
        if self.managers:
            self.logger.debug("There are managers to send to, so get seed list.")
            manager = random.choice(self.managers)
            msg = { 'command': 'init', 'data': { 'groups': self._groups } }
            manager_send = HerdClient(seed=manager)
            response = manager_send.request(msg)
            if response and len(response['data']['groups']):
                self.logger.debug("If we got info back and there were groups to get info from process them.")
                for item in response['data']['groups']:        
                    for value in item.values():
                        if not isinstance(value, list):
                            value = [value]
                        for ip in value:
                            if ip not in seeds and self.address != ip:
                                self.logger.debug("Node with ip (%s) being added..." % ip)
                                seeds.append(ip)

        if seeds:
            self.logger.debug("A list of (%s) seeds were found" % len(seeds))
            self.seeds = seeds
            self._gossiper._seeds = seeds
        return seeds
    
    

