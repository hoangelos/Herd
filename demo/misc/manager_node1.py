from herd.manager.server import HerdManager

manager = HerdManager(address=None, port=8339, ip='127.0.0.1', config=None,
                      stream_ip='127.0.0.1', stream_port=8338)
manager.start_listener()
