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
import os
from unittest import TestCase


from config import Config
import gevent
from gevent.socket import create_connection

class HerdClient(object):
    """The object responsible to manage the user functions of herd."""
    def __init__(self, seed=None, config=None):
        self.logger = logging.getLogger('herd.manager.client')
        self.seed = seed
        self.config = config
        self.load_config()
        self.host, self.port = self.seed.split(':')
    
    def load_config(self):
        """Load the config for connecting to the Herd."""
        # Open the file at default lcoation, unless something else
        # is passed in instead
        self.logger.info('Running load_config() for HerdClient')
        if self.config is not None:
            self.logger.debug("There's a config file passed in")
            f = file(self.config)
            self.cfg = Config(f)
        
            # Allow parameters passed on the command line to override the
            # config file
            if self.seed is None:
                self.logger.debug("There's no seed passed in")
                self.seed = self.cfg.management.seed
    
    def request(self, data):
        """Connect to the manager and send, and optionally receive information."""
        self.logger.info("Sending data to HerdManager")
        response = ""
        s = create_connection((self.host, self.port))
        timeout = gevent.spawn_later(10, lambda: s.close())    
        f = s.makefile()
        f.write(json.dumps(data)+'\n')
        f.flush()
        try:
            response = f.readline()
        except IOError:
            self.logger.debug("The socket couldn't be read from.")
            response = ""
        if response:
            self.logger.debug("There's a response coming back from HerdManager.")
            response = json.loads(response)
        f.close()
        s.close()
        return response
    
