import json
import time

from kafka import KafkaProducer, KafkaConsumer

class kafka_producer():
    def __init__(self):
        self.kafka_topic = 'user_behavior_2'
        self.bootstrap_servers = ['192.168.200.100:9092', '192.168.200.101:9092', '192.168.200.102:9092']
        self.key_serializer = str.encode
        self.value_serializer = lambda v: json.dumps(v).encode('utf-8')
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
                                      key_serializer=self.key_serializer,
                                      value_serializer=self.value_serializer,
                                      compression_type='gzip',
                                      max_request_size=1024 * 1024 * 20,
                                      retries=3)

    def send_csv_into_kafka(self):
        user_file = open('UserBehavior.csv', 'r')
        for line in user_file.readlines():
            data = line.split(',')
            behavior = {
                'user_id': int(data[0]),
                'item_id': int(data[1]),
                'category_id': int(data[2]),
                'behavior': data[3],
                'timestamp': float(data[4])
            }
            self.producer.send(self.kafka_topic, key='user', value=behavior)

class kafka_consumer():
    def __init__(self):
        self.kafka_topic = 'user_behavior'
        self.bootstrap_servers = ['192.168.200.100:9092', '192.168.200.101:9092', '192.168.200.102:9092']
        self.key_serializer = bytes.decode
        self.value_serializer = json.loads
        self.consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                      key_deserializer=self.key_serializer,
                                      value_deserializer=self.value_serializer,
                                      auto_offset_reset='latest')
    def consumer_csv_data(self):
        self.consumer.subscribe([self.kafka_topic])
        for message in self.consumer:
            if message is not None:
                print(message.value['user_id'])



if __name__ == '__main__':
    begin_time = time.time()
    producer = kafka_producer()
    producer.send_csv_into_kafka()
    # consumer = kafka_consumer()
    # consumer.consumer_csv_data()
    print(time.time() - begin_time)
