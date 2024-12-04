from confluent_kafka import Producer
from fastavro.schema import load_schema
from fastavro import writer
import io

producer_config = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(producer_config)

schema = load_schema('user.avsc')

def serialize_to_avro(data, schema):
    bytes_writer = io.BytesIO()
    writer(bytes_writer, schema, [data])
    return bytes_writer.getvalue()

data = {
    "id": 1,
    "name": "Alice",
    "email": "alice@example.com"
}

avro_data = serialize_to_avro(data, schema)
producer.produce('test-topic', value=avro_data)
producer.flush()
