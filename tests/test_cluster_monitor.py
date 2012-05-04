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
import mock

from herd.cluster.monitor import MessagingMonitor

def this_returns(msg):
    return { 'group': msg }

class MonitorTest(TestCase):
    def test_init(self):
        monitor = MessagingMonitor("phalliday", this_returns)
        self.assertEqual(monitor._group_name, "phalliday")
        self.assertEqual(monitor._group_data, this_returns)
    
    def test_name(self):
        monitor = MessagingMonitor("phalliday", this_returns)
        self.assertEqual(monitor.name(), "HerdGroup:phalliday")
    
    def test_value(self):
        monitor = MessagingMonitor("phalliday", this_returns)
        self.assertEqual(monitor.value(), ('msgStack:', json.dumps({ 'group': 'phalliday'})))
    
    def test_value(self):
        monitor = MessagingMonitor("phalliday", this_returns)
        self.assertNotEqual(monitor.generation(), 0)
        monitor._generation = 9999
        self.assertEqual(monitor.generation(), 9999)
        
        
