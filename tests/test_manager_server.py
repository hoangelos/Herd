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

import logging
import os
from unittest import TestCase

import gevent
from gevent.server import StreamServer
import gevent_dht
import mock

from herd.manager.server import HerdManager

class ManagerTest(TestCase):
    def setUp(self):
        real_path = os.path.dirname(os.path.realpath(__file__))
        self.config_path = "%s/configs/herd_manage.conf" % (real_path)
        self.manager = None
    
    def test_load_config(self):
        #Testing fail case
        with self.assertRaises(IOError) as err:
            HerdManager(config='/etc/etc/sdfsdfasdiff.conf')
        missing_exception = err.exception
        self.assertEqual(missing_exception.errno, 2)
        #Testing load works
        self.manager = HerdManager(config=self.config_path)
        self.assertEqual(self.manager.address, None)
        self.assertEqual(self.manager.port, 8339)
        self.assertEqual(self.manager.ip, '127.0.0.1')
        self.assertEqual(self.manager.stream_ip, '0.0.0.0')
        self.assertEqual(self.manager.stream_port, 8338)
        
        self.manager = HerdManager(port=9999, ip='777.666.555.444', stream_ip='111.222.333.444', stream_port=5555)
        self.assertEqual(self.manager.address, None)
        self.assertEqual(self.manager.port, 9999)
        self.assertEqual(self.manager.ip, '777.666.555.444')
        self.assertEqual(self.manager.stream_ip, '111.222.333.444')
        self.assertEqual(self.manager.stream_port, 5555)

        with self.assertRaises(AttributeError):
            HerdManager(config="%s/configs/failed.conf" % (self.real_path))

        
        
    
    def test_parse_msg(self):
        self.manager = HerdManager(config=self.config_path)
        self.manager.table = gevent_dht.distributedHashTable(None)
        #Adding things to the table should show up correctly
        msg = { 'command': 'ping', 'data': { 'ip': '127.0.0.1', 'groups': ['foo', 'bar'] } }
        self.manager._parse_message(msg)
        self.assertEqual(self.manager.table['foo'], ['127.0.0.1'])
        self.assertEqual(self.manager.table['bar'], ['127.0.0.1'])
        self.assertEqual(self.manager.table['group_name'], ['foo', 'bar'])
        
        #You should be able to get the groups
        msg2 = { 'command': 'groups', 'data': None }
        response2 = self.manager._parse_message(msg2)
        self.assertEqual(response2['command'], 'groups')
        self.assertEqual(response2['data']['groups'], ['foo', 'bar'])
        
        #Uupdating the groups should update the groups
        msg3 = { 'command': 'ping', 'data': { 'ip': '127.0.0.2', 'groups': ['baz'] } }
        self.manager._parse_message(msg3)
        self.assertEqual(self.manager.table['baz'], ['127.0.0.2'])
        self.assertEqual(self.manager.table['group_name'], ['foo', 'bar', 'baz'])
        response3 = self.manager._parse_message(msg2)
        self.assertEqual(response3['command'], 'groups')
        self.assertEqual(response3['data']['groups'], ['foo', 'bar', 'baz'])
        
        #Test getting the IPs message
        msg4 = { 'command': 'init', 'data': { 'groups': ['foo', 'bar', 'baz'] } }
        response4 = self.manager._parse_message(msg4)
        self.assertEqual(response4['command'], 'init')
        
        self.assertEqual(response4['data']['groups'][0]['foo'], ['127.0.0.1'])
        self.assertEqual(response4['data']['groups'][1]['bar'], ['127.0.0.1'])
        self.assertEqual(response4['data']['groups'][2]['baz'], ['127.0.0.2'])
        
        #Test getting managers
        msg5 = { 'command': 'managers', 'data': None }
        response5 = self.manager._parse_message(msg5)
        self.assertEqual(response5['data']['managers'], ['127.0.0.1:8339'])
    
    def test_start_listener(self):
        self.manager = HerdManager(port=9999, ip='127.0.0.1', stream_ip='127.0.0.1', stream_port=5555)
        with mock.patch('herd.manager.server.StreamServer') as MockClient:
            instance = MockClient.return_value
            instance.serve_forever.return_value = None
            self.manager.start_listener()
            self.assertEqual(MockClient.called, True)
            self.assertEqual(instance.serve_forever.called, True)
    
    def test_stop_listener(self):
        self.manager = HerdManager(port=10000, ip='127.0.0.1', stream_ip='127.0.0.1', stream_port=6666)
        with mock.patch('herd.manager.server.StreamServer') as MockClient:
            instance = MockClient.return_value
            instance.serve_forever.return_value = None
            instance.stop.return_value = None
            self.manager.start_listener()
            self.assertEqual(MockClient.called, True)
            self.assertEqual(instance.serve_forever.called, True)
            self.manager.stop_listener()
            self.assertEqual(instance.stop.called, True)
        
        
    
