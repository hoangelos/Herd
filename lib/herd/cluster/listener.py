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
import re

from elastica.listeners import BaseListener

class MessageListener(BaseListener):
    """Listen for actions like new nodes, nodes dying, and data changing."""
    def __init__(self, change_callback=None):
        self.logger = logging.getLogger('herd.cluster.listener')
        self.logger.info('init for listener')
        self.change_callback = change_callback
        super(MessageListener, self).__init__()
    
    def on_join(self, host):
        """A node joins the cluster. Right now we ignore except for logging it."""
        self.logger.info("Listener sees on_join() for host (%s)" % host)
    
    def on_alive(self, host):
        """A node is seen as alive. Right now we ignore except for logging it."""
        self.logger.info("Listener sees on_alive() for host (%s)" % host)
    
    def on_dead(self, host):
        """A node is dead. Right now we ignore except for logging it."""
        self.logger.info("Listener sees on_dead() for host (%s)" % host)
    
    def on_change(self, host, name, old_value, new_value):
        """Data has changed from a node."""
        self.logger.info("Listener sees on_change() for host (%s) and name (%s)" % (host, name))
        group = re.sub('HerdGroup:', '', name)
        msg = re.sub('MsgStack:', '', new_value)
        translated = json.loads(msg)
        if self.change_callback:
            self.logger.debug("A callback is being attempted")
            try:
                self.change_callback(group, translated)
            except (AttributeError, TypeError):
                self.logger.exception("The callback is not callable.")
            except Exception, e:
                self.logger.exception("An error occured while calling the callback.")
        
