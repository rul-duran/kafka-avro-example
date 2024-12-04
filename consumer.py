from confluent_kafka import Consumer
from fastavro import reader
import io

consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test-group',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_config)
consumer.subscribe(['test-topic'])

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    bytes_reader = io.BytesIO(msg.value())
    avro_data = list(reader(bytes_reader))
    print(f"Mensaje recibido: {avro_data}")
