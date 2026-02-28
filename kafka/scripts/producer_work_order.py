import csv
import json
import time
# import random
from confluent_kafka import Producer, avro
from confluent_kafka.avro import AvroProducer
from datetime import datetime

def convert_to_epoch_ms(dt_str):
    # Example input: "13/11/2025 07:17"
    dt = datetime.strptime(dt_str, "%d/%m/%Y %H:%M")
    return int(dt.timestamp() * 1000)

def delivery_report(err, msg):
   if err is not None:
       print(f"Pesan tidak terkirim: {err}")
   else:
       print(f"Pesan terkirim ke {msg.topic()} [{msg.partition()}] @ {msg.offset()}")

key_schema = {
    "namespace": "wo_avro",
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "wo_id", "type": "string"}
    ] 
}

value_schema = {
    "namespace": "wo_avro",
    "type": "record",
    "name": "Value",
    "fields": [
        {"name": "wo_id", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "ownership", "type": "string"},
        {"name": "ship_to", "type": "string"},
        {"name": "order_datetime", "type": {"type": "long", "logicalType": "timestamp-millis"}}
    ] 
}

producer = AvroProducer(
    config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081"
    },
    default_key_schema = avro.loads(json.dumps(key_schema)),
    default_value_schema = avro.loads(json.dumps(value_schema))
)

# producer = Producer({
#    "bootstrap.servers": "localhost:9092",
# })

source = 'data/WO_Transaction.csv'

with open(source, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=';')
    while True:
        f.seek(0)
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            producer.poll(0)
            producer.produce(
                topic = "work_order",
                key = {"wo_id": row.get("wo_id")},
                value = {
                    "wo_id": row.get("wo_id"),
                    "vehicle_id": row.get("vehicle_id"),
                    "ownership": row.get("ownership"),
                    "ship_to": row.get("ship_to"),
                    "order_datetime": convert_to_epoch_ms(row.get("order_datetime")),
                },
                callback = delivery_report,
            )

            time.sleep(10)

        producer.flush()
        time.sleep(10)

