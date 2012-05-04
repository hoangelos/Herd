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

import mock

from herd.cluster.server import HerdCluster
import herd

class ClusterTest(TestCase):
    def setUp(self):
        real_path = os.path.dirname(os.path.realpath(__file__))
        self.config_path = "%s/configs/herd_cluster.conf" % (real_path)
    
    def test_load_config(self):
        #Testing fail case
        with self.assertRaises(IOError) as err:
            self.cluster = HerdCluster(config='/etc/etc/sdfsdfasdiff.conf')
            self.cluster._sock.close()
        missing_exception = err.exception
        self.assertEqual(missing_exception.errno, 2)
        
        #Testing load works
        self.cluster = HerdCluster(config=self.config_path)
        self.assertEqual(self.cluster.address, "127.0.0.1:77777")
        self.assertEqual(self.cluster.seeds[0], '127.0.0.2:77777')
        self.assertEqual(self.cluster.seeds[1], '127.0.0.3:77777')
        self.assertEqual(self.cluster._groups[0], 'phalliday')
        self.assertEqual(self.cluster.ping_time, 600000)
        self.assertEqual(self.cluster.managers[0], '127.0.0.4:88888')
        self.cluster._sock.close()
        
        #Testing passing arguments in
        self.cluster = HerdCluster(seeds=["127.0.0.2:8888"], address="127.0.0.3:8899", groups=['asdf'], ping_time=11, managers=['127.0.0.44:9999'], config=self.config_path)
        self.assertEqual(self.cluster.address, "127.0.0.3:8899")
        self.assertEqual(self.cluster.seeds, ["127.0.0.2:8888"])
        self.assertEqual(self.cluster._groups[0], 'asdf')
        self.assertEqual(self.cluster.ping_time, 11)
        self.assertEqual(self.cluster.managers[0], '127.0.0.44:9999')
        self.cluster._sock.close()
        
        with self.assertRaises(AttributeError):
            self.cluster = HerdCluster(config="%s/configs/failed.conf" % (self.real_path))
            self.cluster._sock.close()
        
    
    def test_get_local_data(self):
        self.cluster = HerdCluster(config=self.config_path)
        self.cluster._local_data['foo'] = 'bar'
        self.cluster._groups = ['foo']
        self.assertEqual(self.cluster.get_local_data('foo'), 'bar')
        self.assertEqual(self.cluster.get_local_data('baz'), None)
        self.cluster._sock.close()
    
    def test_update_local_data(self):
        self.cluster = HerdCluster(config=self.config_path)
        self.cluster._local_data['foo'] = 'bar'
        self.cluster._groups = ['foo']
        self.cluster.update_local_data('foo', 'baz')
        self.assertEqual(self.cluster._local_data['foo'], 'baz')
        self.cluster._groups = ['baz']
        self.cluster.update_local_data('baz', 'foo')
        self.assertEqual(self.cluster._local_data['baz'], 'foo')
        self.cluster._sock.close()
    
    def test_add_groups(self):
        self.cluster = HerdCluster(config=self.config_path)
        self.assertEqual(len(self.cluster._groups), 1)
        self.assertEqual(self.cluster.add_group('blah'), True)
        self.assertEqual(len(self.cluster._groups), 2)
        self.assertEqual('blah' in self.cluster._groups, True)
        self.cluster._sock.close()
    
    def test_remove_group(self):
        self.cluster = HerdCluster(config=self.config_path)
        self.assertEqual(len(self.cluster._groups), 1)
        self.cluster._groups.append('foo')
        test = { 'foo': int() }
        self.cluster._gossiper._application_state_publishers.append(test['foo'])
        self.cluster._publishers['foo'] = test['foo']
        self.assertEqual(len(self.cluster._groups), 2)
        self.assertEqual(self.cluster.remove_group('foo'), True)
        self.assertEqual(len(self.cluster._groups), 1)
        self.assertEqual(self.cluster.remove_group('blah'), False)
        self.cluster._sock.close()
    
    def test_ping_managers(self):
        with mock.patch('herd.cluster.server.HerdClient') as MockClient:
            instance = MockClient.return_value
            instance.request.return_value = 'foo'      
            self.cluster = HerdCluster(config=self.config_path)
            self.assertEqual(len(self.cluster.managers), 1)
            sent_to = self.cluster._ping_managers()
            self.assertEqual(sent_to, '127.0.0.4:88888')
            self.assertEqual(MockClient.called, True)
            self.assertEqual(instance.request.called, True)
            instance.request.assert_called_once_with({ 'command': 'ping', 'data': { 'ip': '127.0.0.1:77777', 'groups': [ 'phalliday'] } })
            MockClient.assert_called_once_with(seed='127.0.0.4:88888')
            self.cluster._sock.close()
