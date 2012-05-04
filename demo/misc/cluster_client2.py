from herd.cluster.client import HerdClusterClient

msg = { 'get_local_data': 'phalliday' }
client = HerdClusterClient(seeds=['127.0.0.3'])
client.request(msg)
