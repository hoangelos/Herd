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

from elastica.monitors import MonitorBase


class MessagingMonitor(MonitorBase):
    """This is what's used to send updates on local data changes to the rest of the cluster."""
    def __init__(self, group=None, group_data_cb=None):
        self.logger = logging.getLogger('herd.cluster.monitor')
        self._group_name = group
        self._group_data = group_data_cb
        super(MessagingMonitor, self).__init__()
    
    def name(self):
        """The name of the Monitor, in this case the group."""
        self.logger.info('Calling name() of monitor for group (%s)' % self._group_name)
        return "HerdGroup:%s" % (self._group_name)
    
    def value(self):
        """This is the messages that were updated."""
        data = self._group_data(self._group_name)
        payload = json.dumps(data)
        self.logger.info('Calling value() of monitor with payload of %s' % payload)
        return ("msgStack:", payload)
    
    def generation(self):
        """This uses a default timestamp."""
        return self._generation
    