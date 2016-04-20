#!/usr/bin/python
import yaml
import argparse
from kafka import KafkaConsumer
import imp
import json
import sys

parser = argparse.ArgumentParser(description='Process cache relay commands from Kafka')
parser.add_argument('--config', required=True, help='YAML configuration file')
script_args = parser.parse_args()

config_file = open(script_args.config)
config = yaml.safe_load(config_file)

if 'listeners' not in config:
    raise Exception('listeners config required')

topics = []
handlers = {}
for listener in config['listeners']:
    topics.append(listener['topic'])
    fp, pathname, description = imp.find_module(listener['handler'], ['handlers'])
    mod = imp.load_module(listener['handler'], fp, pathname, description)
    klass = getattr(mod, listener['handler'])
    handlers[listener['topic']] = klass(**listener['params'])

consumer = KafkaConsumer(*topics)
for msg in consumer:
    if msg.topic not in handlers:
        print("Weird, unknown topic %s" % msg.topic)
        continue
    try:
        data = json.loads(msg.value)
    except ValueError:
        data = None
    if not data:
        print("Could not parse data, meh")
        continue
    try:
    handlers[msg.topic].handle(msg.topic, data)
    except:
        e = sys.exc_info()
        print("Oops, something happened: " + str(e))
