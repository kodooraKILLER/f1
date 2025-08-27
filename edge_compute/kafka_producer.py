import ssl
from quixstreams import Application
import json

class kafkaMonster:
    def __init__(self,topic_name,loglevel="INFO"):
        self.topic_name = topic_name
        self.loglevel = loglevel
        app = Application(
            broker_address="localhost:9092",
            loglevel=self.loglevel
        )
        self.producer = app.get_producer()

    def produce(self,key,value):              
        self.producer.produce(
            topic = self.topic_name,
            key = key,
            value = json.dumps(value)
        )
        if self.loglevel in ["DEBUG"]:
            print(f"Produced {key} data into topic {self.topic_name}")
            return True
    def flusher(self):              
        self.producer.flush()
        if self.loglevel in ["DEBUG"]:
            print(f"Flushed the topic")

