from __future__ import print_function

from pymemcache.client.base import Client
from pymemcache.exceptions import MemcacheUnexpectedCloseError
import time


class Memcached(object):
    DELAY = 0.5
    DEBUG = False

    def __init__(self, hostname, port, **params):
        self.mc = Client((hostname, port))

    def handle(self, topic, message):
        """
        """
        if 'cmd' not in message:
            raise Exception("Bad message: no command")
        cmd = message['cmd']
        if not hasattr(self, cmd):
            raise Exception("Unknown command: " + cmd)
        tryit = True
        while tryit:
            tryit = False
            try:
                getattr(self, cmd)(message)
            except MemcacheUnexpectedCloseError:
                # Server dropped dead - we'll retry
                tryit = True
            except IOError:
                # Something network-related - retry
                tryit = True
            if tryit:
                time.sleep(self.DELAY)

    def set(self, message):
        text = message['val'].encode('utf-8')
        if message.get('sbt', None):
            purge_time = time.time() + message.get('uto', 0)
            text = text.replace('$UNIXTIME$', '%.6f' % purge_time)
        if self.DEBUG:
            print("Set {0}-{1}-{2}".format(message['key'].encode('utf-8'), text, int(message['ttl'])))
        self.mc.set(message['key'].encode('utf-8'), text, int(message['ttl']))

    def delete(self, message):
        self.mc.delete(message['key'])
