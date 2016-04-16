import memcache

class Memcached:

    def __init__(self, hostname, **params):
        self.mc = memcache.Client([hostname])

    def handle(self, topic, message):
        cmd = message['cmd']
        if not hasattr(self, cmd):
            raise Exception("Unknown command: " + cmd)
        getattr(self, cmd)(message)

    def set(self, message):
        print("Set {0}-{1}-{2}".format(message['key'].encode('utf-8'), message['val'].encode('utf-8'), int(message['ttl'])))
        self.mc.set(message['key'].encode('utf-8'), message['val'].encode('utf-8'), int(message['ttl']) )

    def delete(self, message):
        self.mc.delete(message['key'])