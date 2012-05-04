from herd.cluster.client import HerdClusterClient

msg = { 'update_local_data': 'phalliday<===>What about now?'}
client = HerdClusterClient(seeds=['127.0.0.2'])
client.request(msg)
msg2 = { 'get_local_data': 'phalliday' }
