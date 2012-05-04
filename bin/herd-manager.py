#!/usr/bin/env python
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

import gevent

from herd.manager.server import HerdManager
from herd import herd_config

def manager1(args):
    #Get options from command-line
    manager = HerdManager(address=args.address, port=args.port, ip=args.ip, config=args.config,
                          stream_ip=args.stream_ip, stream_port=args.stream_port)
    manager.start_listener()

def main(args):
    gevent.joinall([
        gevent.spawn(manager1, args)  
    ])
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser("Start a Herd Manager node")
    parser.add_argument("-a", "--address", dest="address", default=None,
                        help="Address to connect to of existing node. In format of xxx.xxx.xxx.xxx[:nnnn].",
                        metavar="ADDRESS")
    parser.add_argument('-p', "--port", dest="port", default=None,
                        help="Port to connect to locally. Default:8339",
                        metavar="PORT")
    parser.add_argument('-i', "--ip", dest="ip", default=None, metavar="IP_ADDRESS",
                        help="IP address to connect to locally. Default: 127.0.0.1")
    parser.add_argument('-I', '--stream_ip', dest="stream_ip", default=None, metavar="IP_ADDRESS",
                        help="IP address to connect the stream too. Default: 0.0.0.0")
    parser.add_argument('-P', '--stream_port', dest="stream_port", default=None,
                        help="Port to connect the stream to. Default:8339",
                        metavar="PORT")
    config_help = "Config file PATH if different than default. (Default: %s)" % herd_config.HERD_MANAGE_CONFIG
    parser.add_argument('-c', "--config", dest="config", default=None, metavar="FILE",
                        help=config_help)
    args = parser.parse_args()
    main(args)
