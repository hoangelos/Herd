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

import argparse
from exceptions import IOError
import json
import logging
import os
import signal
import time
from unittest import TestCase

from config import Config
import gevent
import gevent_dht
from geventdaemon import GeventDaemonContext
from gevent.server import StreamServer

from herd import herd_config

class HerdManager(object):
    """The nodes that keep track of where the groups are."""
    def __init__(self, address=None, port=None, ip=None, config=None, stream_ip=None, stream_port=None):
        self.logger = logging.getLogger("herd.manager.server")
        self.context = None
        self.stream = None
        self.address = address
        if port:
            self.port = int(port)
        else:
            self.port = None
        self.ip = ip
        self.config = config
        self.stream_ip = stream_ip
        if stream_port:
            self.stream_port = int(stream_port)
        else:
            self.stream_port = None
        self._load_config()
    
    def _load_config(self):
        """Load up the config to load up the right port, ip, and where 
        to connect.
        """
        # Open the file at default lcoation, unless something else
        # is passed in instead
        self.logger.info("loading config for HerdManager")
        if self.config is not None:
            self.logger.debug("A config file will attempt to be loaded")
            f = file(self.config)
            self.cfg = Config(f)
        
            # Allow parameters passed on the command line to override the
            # config file
            if self.address is None:
                self.logger.debug("An address will be loaded from config")
                self.address = self.cfg.networking.address
            if self.port is None:
                self.logger.debug("A port will be loaded from config")
                self.port = int(self.cfg.networking.port)
            if self.ip is None:
                self.logger.debug("An ip will be loaded from config")
                self.ip = self.cfg.networking.ip
            if self.stream_port is None:
                self.logger.debug("A stream port will be loaded from config")
                self.stream_port = int(self.cfg.stream.port)
            if self.stream_ip is None:
                self.logger.debug("A stream ip will be loaded from config")
                self.stream_ip = self.cfg.stream.ip
    
    def _handle_request(self, socket, address):
        """Handle the request for the server"""
        self.logger.info("Handling the request sent from a client")
        timeout = gevent.spawn_later(10, lambda: socket.shutdown(0))
        response = None         
        fileobj = socket.makefile()
        line = fileobj.readline()
        timeout.kill()
        if line:
            self.logger.debug("Getting a line")
            msg = json.loads(line)
            response = self._parse_message(msg)
            if response:
                self.logger.debug("Sending a response back")
                fileobj.write(json.dumps(response))
                fileobj.flush()
        return response 
    
    def _parse_message(self, message):
        """Parse the message."""
        self.logger.info("Parsing a message sent by a HerdClient")
        response = None
        
        #Handle the ping, which let's us know what machines are still in the group
        if message['command'] == u'ping':
            self.logger.info("Processing a PING message")
            for group in message['data']['groups']:
                #Add the IP if it hasn't already been added
                error = False
                try:
                    self.table[group]
                except TypeError:
                    self.table[group] = []
                
                if not error:
                    if not self.table[group] or message['data']['ip'] not in self.table[group]:
                        self.logger.debug("Adding ip (%s) to group (%s)" % (message['data']['ip'], group))
                        self.table.append(group, message['data']['ip'])
                    #Add the group name if we don't already have it.
                    if not self.table['group_name'] or group not in self.table['group_name']:
                        self.logger.debug("A new group was added (%s)" % group)
                        self.table.append('group_name', group)
        
        #Get the name of all the groups
        elif message['command'] == u'groups':
            self.logger.info("Processing a GROUPS message")
            groups = self.table['group_name']
            response = { 'command': 'groups', 'data': { 'groups': groups } }
        
        #Get all the members of a list of groups
        elif message['command'] == u'init':
            self.logger.info("Processing an INIT message")
            response = { 'command': 'init', 'data': { 'groups': [] } }
            for group in message['data']['groups']:
                if self.table[group]:
                    response['data']['groups'].append({group: self.table[group]})
        
        #Get a bunch of managers that can be contacted
        elif message['command'] == u'managers':
            self.logger.info("Processing a MANAGERS message")
            response = { 'command': 'managers', 'data': { 'managers': self.get_managers() } }
        
        return response
    
    def get_managers(self):
        """Return the list of DHT nodes that are in the finger table.  
        This isn't everyone, but will be  a good starting point"""
        self.logger.info("Getting the managers")
        return self.table.listener.finger.get_nodes()
    
    def start_listener(self, forever=True):
        """Start this node and attach it to the DHT."""
        self.logger.info('Starting HerdManager node...')
        self.stream = StreamServer((self.stream_ip, self.stream_port), self._handle_request)
        gevent.signal(signal.SIGTERM, self.stop_listener)
        gevent.signal(signal.SIGQUIT, self.stop_listener)
        gevent.signal(signal.SIGINT, self.stop_listener)
        self.table = gevent_dht.distributedHashTable(self.address, self.port, self.ip)
        if forever:
            self.stream.serve_forever()
        else:
            self.stream.start()

    def stop_listener(self):
        """Call to stop the node from the DHT."""
        self.logger.info("Stopping the HerdManager node...")
        self.stream.stop()