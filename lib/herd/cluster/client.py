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
import socket

from tornado import ioloop
from tornado import iostream

class HerdClusterClient(object):
    """The client to connect to the HerdCluster nodes.  This gets messages that are being subscribed to."""
    def __init__(self, seeds=[], port=8888):
        self.logger = logging.getLogger("herd.cluster.client")
        self.seeds = seeds
        self.port = port
        
    def request(self, msg, closing_callback=None):
        """Send a message to one of the HerdCluster nodes."""
        self.logger.info("Requesting data from HerdCluster")
        seed = random.choice(self.seeds)
        self._msg = msg
        self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._stream = iostream.IOStream(self._s)
        self._stream.connect((seed, self.port), self._send_request)
        if closing_callback is not None:
            self._stream.set_close_callback(closing_callback)
        ioloop.IOLoop.instance().start()
    
    def _send_request(self):
        """Perform the sending of the data to the HerdCluster nodes."""
        self.logger.info("Sending the data to the HerdCluster")
        msg = json.dumps(self._msg)
        self._stream.write(msg+"\n\n")
        self._stream.read_until('\n\n', self._get_response)
    
    def _get_response(self, data):
        """Act on the data that gets received from the Cluster."""
        self.logger.info("Receiving data from HerdCluster (%s)" % data)
        if data:
            print data
        self._stream.close()
        ioloop.IOLoop.instance().stop()