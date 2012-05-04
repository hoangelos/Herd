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
import sys
from unittest import TestCase

import mock

from herd.cluster.listener import MessageListener
 
def broken_callback(arg1, arg2):
    return asd()

class NotCallable():
    pass

class MockLoggingHandler(logging.Handler):
    """Mock logging handler to check for expected logs."""

    def __init__(self, *args, **kwargs):
        self.reset()
        logging.Handler.__init__(self, *args, **kwargs)

    def emit(self, record):
        self.messages[record.levelname.lower()].append(record.getMessage())

    def reset(self):
        self.messages = {
            'debug': [],
            'info': [],
            'warning': [],
            'error': [],
            'critical': [],
        }

class ListenerTest(TestCase):    
    def test_init_listener(self):
        logger_under_test = logging.getLogger('herd.cluster.listener')
        listener = MessageListener()
        self.assertEqual(listener.change_callback, None)
        self.assertEqual(listener.logger.name, logger_under_test.name)
        self.assertEqual(listener.logger, logger_under_test)
        listener2 = MessageListener(change_callback=broken_callback)
        self.assertEqual(listener2.change_callback, broken_callback)
        self.assertEqual(listener2.logger.name, logger_under_test.name)
        self.assertEqual(listener2.logger, logger_under_test)
    
    def test_on_join(self):
        with mock.patch('herd.cluster.listener.logging.getLogger') as MockedObject:
            instance = MockedObject.return_value
            instance.info.return_value = None
            instance.exception.return_value = None
            listener = MessageListener()
            self.assertEquals(MockedObject.called, True)
            listener.on_join('777.666.555.444')
            self.assertEquals(instance.info.called, True)
            self.assertEqual(instance.exception.called, False)
        logger = logging.getLogger('herd')
        logger.setLevel(logging.DEBUG)
        handler = MockLoggingHandler()
        logger.addHandler(handler)
        listener = MessageListener()
        self.assertEqual(len(logger.handlers[0].messages['info']), 1)
        listener.on_join('777.666.555.444')
        self.assertEqual(len(logger.handlers[0].messages['info']), 2)
        self.assertEqual(len(logger.handlers[0].messages['debug']), 0)
        self.assertEqual(len(logger.handlers[0].messages['critical']), 0)
        self.assertEqual(len(logger.handlers[0].messages['error']), 0)
        self.assertEqual(len(logger.handlers[0].messages['warning']), 0)
        self.assertEqual(logger.handlers[0].messages['info'][1], "Listener sees on_join() for host (777.666.555.444)")
        handler.close()
        logger.removeHandler(handler)
        
        
    
    def test_on_alive(self):
        with mock.patch('herd.cluster.listener.logging.getLogger') as MockedObject:
            instance = MockedObject.return_value
            instance.info.return_value = None
            instance.exception.return_value = None
            listener = MessageListener()
            self.assertEquals(MockedObject.called, True)
            listener.on_alive('777.666.555.444')
            self.assertEquals(instance.info.called, True)
            self.assertEqual(instance.exception.called, False)
        logger = logging.getLogger('herd')
        logger.setLevel(logging.DEBUG)
        handler = MockLoggingHandler()
        logger.addHandler(handler)
        listener = MessageListener()
        self.assertEqual(len(logger.handlers[0].messages['info']), 1)
        listener.on_alive('777.666.555.444')
        self.assertEqual(len(logger.handlers[0].messages['info']), 2)
        self.assertEqual(len(logger.handlers[0].messages['debug']), 0)
        self.assertEqual(len(logger.handlers[0].messages['critical']), 0)
        self.assertEqual(len(logger.handlers[0].messages['error']), 0)
        self.assertEqual(len(logger.handlers[0].messages['warning']), 0)
        self.assertEqual(logger.handlers[0].messages['info'][1], "Listener sees on_alive() for host (777.666.555.444)")
        handler.close()
        logger.removeHandler(handler)
        

    
    def test_on_dead(self):
        with mock.patch('herd.cluster.listener.logging.getLogger') as MockedObject:
            instance = MockedObject.return_value
            instance.info.return_value = None
            instance.exception.return_value = None
            listener = MessageListener()
            self.assertEquals(MockedObject.called, True)
            listener.on_dead('777.666.555.444')
            self.assertEquals(instance.info.called, True)
            self.assertEqual(instance.exception.called, False)
        logger = logging.getLogger('herd')
        logger.setLevel(logging.DEBUG)
        handler = MockLoggingHandler()
        handler.reset()
        logger.addHandler(handler)
        listener = MessageListener()
        self.assertEqual(len(logger.handlers[0].messages['info']), 1)
        listener.on_dead('777.666.555.444')
        self.assertEqual(len(logger.handlers[0].messages['info']), 2)
        self.assertEqual(len(logger.handlers[0].messages['debug']), 0)
        self.assertEqual(len(logger.handlers[0].messages['critical']), 0)
        self.assertEqual(len(logger.handlers[0].messages['error']), 0)
        self.assertEqual(len(logger.handlers[0].messages['warning']), 0)
        self.assertEqual(logger.handlers[0].messages['info'][1], "Listener sees on_dead() for host (777.666.555.444)")
        handler.close()
        logger.removeHandler(handler)


    
    def test_on_change(self):
        json_msg = { 'blah': 'new value' }
        msg = json.dumps(json_msg)
        with mock.patch('herd.cluster.listener.logging.getLogger') as MockedObject:
            instance = MockedObject.return_value
            instance.info.return_value = None
            instance.exception.return_value = None
            listener = MessageListener()
            self.assertEquals(MockedObject.called, True)
            listener.on_change('777.666.555.444', 'test', 'old value', msg)
            self.assertEquals(instance.info.called, True)
            self.assertEqual(instance.exception.called, False)
            listener2 = MessageListener(change_callback=broken_callback)
            listener2.on_change('777.666.555.444', 'test', 'old value', msg)
            self.assertEquals(instance.info.called, True)
            self.assertEqual(instance.exception.called, True)
            listener3 = MessageListener(change_callback=1)
            listener3.on_change('777.666.555.444', 'test', 'old value', msg)
            self.assertEquals(instance.info.called, True)
            self.assertEqual(instance.exception.called, True)
        logger = logging.getLogger('herd')
        logger.setLevel(logging.DEBUG)
        handler = MockLoggingHandler()
        logger.addHandler(handler)
        listener = MessageListener()
        self.assertEqual(len(logger.handlers[0].messages['info']), 1)
        listener.on_change('777.666.555.444', 'test', 'old value', msg)
        self.assertEqual(len(logger.handlers[0].messages['info']), 2)
        self.assertEqual(len(logger.handlers[0].messages['debug']), 0)
        self.assertEqual(len(logger.handlers[0].messages['critical']), 0)
        self.assertEqual(len(logger.handlers[0].messages['error']), 0)
        self.assertEqual(len(logger.handlers[0].messages['warning']), 0)
        self.assertEqual(logger.handlers[0].messages['info'][1], "Listener sees on_change() for host (777.666.555.444) and name (test)")
        handler.reset()
        listener = MessageListener(change_callback=broken_callback)
        listener.on_change('777.666.555.444', 'test', 'old value', msg)
        self.assertEqual(len(logger.handlers[0].messages['error']), 1)
        self.assertEqual(logger.handlers[0].messages['error'][0], "An error occured while calling the callback.")
        handler.reset()
        listener = MessageListener(change_callback=1)
        listener.on_change('777.666.555.444', 'test', 'old value', msg)
        self.assertEqual(len(logger.handlers[0].messages['error']), 1)
        self.assertEqual(logger.handlers[0].messages['error'][0], "The callback is not callable.")
        handler.close()
        logger.removeHandler(handler)
        

            


