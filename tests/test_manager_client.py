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

import gevent

from herd.manager.client import HerdClient

class ClientTest(TestCase):
    def setUp(self):
        self.real_path = os.path.dirname(os.path.realpath(__file__))
        self.config_path = "%s/configs/herd_client.conf" % (self.real_path)
    
    def test_load_config(self):
        #Testing fail case
        with self.assertRaises(IOError) as err:
            HerdClient(config='/etc/etc/sdfsdfasdiff.conf')
        missing_exception = err.exception
        self.assertEqual(missing_exception.errno, 2)
        #Testing load works
        self.client = HerdClient(config=self.config_path)
        self.assertEqual(self.client.seed, "127.0.0.1:8338")
        #Testing passing arguments in
        self.client = HerdClient(seed="127.0.0.2:8888", config=self.config_path)
        self.assertEqual(self.client.seed, "127.0.0.2:8888")
        with self.assertRaises(AttributeError):
            HerdClient(config="%s/configs/failed.conf" % (self.real_path))
            
    
    def test_request(self):
        from gevent.server import StreamServer
        def handle_req(sock, address):
            timeout = gevent.spawn_later(10, lambda: sock.shutdown(0))            
            fileobj = sock.makefile()
            
            while True:
                try:
                    line = fileobj.readline()
                except IOError:
                    line = None
                timeout.kill()
                if not line:
                    break
                else:
                    msg = json.loads(line)
                    msg['foo'] = 'bar'
                    if msg:
                        fileobj.write(json.dumps(msg))
                        fileobj.flush()
                fileobj.close()
                sock.close()
                break
        
        self.stream = StreamServer(('127.0.0.1', 8338), handle_req)
        self.stream.start()
        
        self.client = HerdClient(seed="127.0.0.1:8338")
        msg = { 'testing': 'this' }
        response = self.client.request(msg)
        self.assertEqual(response['foo'], 'bar')
        self.assertEqual(response['testing'], 'this')
        self.stream.stop()
