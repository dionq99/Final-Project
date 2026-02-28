from confluent_kafka import Consumer, avro
from confluent_kafka.avro import AvroConsumer
from database import get_session
from models.work_orders import eta, Work_Orders
from sqlmodel import Session, select

# Functions
def lookup_eta(ownership: str, ship_to: str) -> int:
   with get_session() as session:
      statement = (
         select(eta)
         .where(eta.ownership == ownership)
         .where(eta.ship_to == ship_to)
      )
      result = session.exec(statement).first()

      if result:
         return result.eta
      else:
         return None

def update_work_order(msg):
   data = msg.value()
   eta_value = lookup_eta(
      ownership=data['ownership'],
      ship_to=data['ship_to']
   )
   if eta_value is None:
      print(f"[WARN] ETA not found for work order {data['wo_id']}")

   record = Work_Orders(
      wo_id = data['wo_id'],
      vehicle_id = data['vehicle_id'],
      ownership = data['ownership'],
      ship_to = data['ship_to'],
      order_datetime = data['order_datetime'],
      eta = eta_value
   )
   with get_session() as session:
      session.merge(record)
      session.commit()
      # session.close()

   print("Recorded work order:", record.wo_id)

# Konfigurasi consumer
consumer = AvroConsumer({
   "bootstrap.servers": "localhost:9092",
   "group.id"         : "wo",
   "schema.registry.url": "http://localhost:8081"
})

# Subscribe to topic
consumer.subscribe(["work_order"])
try:
   while True:
         msg = consumer.poll(timeout=1)  # Wait 1 second for messages
         if msg is None:
            continue
         if msg.error():
            print("Error: ", msg.error())
         update_work_order(msg)

finally:
   consumer.close()


   