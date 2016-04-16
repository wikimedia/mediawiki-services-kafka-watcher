# Kafka listener

This is a simple Python daeom listening to Kafka topics and
processing the messages. Configured by YAML file. Example:

    listeners:
      - topic: wancache-purge
        handler: Memcached
        params:
          hostname: localhost:11211

Handler classes are expected to be in handlers directory. 
