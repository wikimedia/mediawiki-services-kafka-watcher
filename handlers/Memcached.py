import memcache

class Memcached(object):
    def __init__(self, hostname, **params):
        self.mc = memcache.Client([hostname])

    def handle(self, topic, message):
        cmd = message['cmd']
        if not hasattr(self, cmd):
            raise Exception("Unknown command: " + cmd)
        getattr(self, cmd)(message)

    def set(self, message):
        text = message['val'].encode('utf-8')
        if message.get('sbt', None):
            purge_time = time.time() + message.get('uto', 0)
            text = text.replace('$UNIXTIME$', '%.6f' % purge_time)
#        print("Set {0}-{1}-{2}".format(message['key'].encode('utf-8'), text, int(message['ttl'])))
        self.mc.set(message['key'].encode('utf-8'), text, int(message['ttl']) )

    def delete(self, message):
        self.mc.delete(message['key'])