class Dump(object):

    def __init__(self, **params):
        print(params)

    def handle(self, topic, message):
        print(topic)
        print(message)
