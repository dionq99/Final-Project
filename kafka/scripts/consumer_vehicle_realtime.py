from confluent_kafka import Consumer, avro
from confluent_kafka.avro import AvroConsumer
from database import get_session
from datetime import datetime, timezone
from models.vehicle import Work_Orders, VehicleRealtime
from sqlmodel import Session, select
import geopandas as gpd

speed_limit = 50

# Functions
def is_late(duration, eta):
   if duration > eta:
      return 1

def is_overspeed(speed):
   if speed > speed_limit:
      return 1

def lookup_wo(vehicle_id: str) -> int:
   with get_session() as session:
      statement = (
         select(Work_Orders)
         .where(Work_Orders.vehicle_id == vehicle_id)
      )
      result = session.exec(statement).first()

      if result:
         return result
      else:
         return None

def get_boundary(office_id):
   source = 'data/Offices.shp'
   gdf = gpd.read_file(source)
   gdf = gdf[gdf['idmill']==office_id]
   return gdf['geometry']

def get_status(x):
   owner_boundary = get_boundary(x['ownership'])
   shipto_boundary = get_boundary(x['ship_to'])
   x_geom = gpd.points_from_xy(x["longitude"], x["latitude"])
   if x_geom.within(owner_boundary):
      return "Loading"
   elif x_geom.within(shipto_boundary):
      return "Finish"
   else:
      return "Shipping"

def update_vehicle_realtime(msg):
   data = msg.value()
   wo_info = lookup_wo(
      vehicle_id=data['vehicle_id'])
   
   if wo_info is None:
      print(f"[WARN] No work order for {data['vehicle_id']}")
      return

   eta = wo_info.eta

   duration_min = (data['process_datetime'] - datetime.combine(wo_info.order_datetime, datetime.min.time().replace(tzinfo=timezone.utc))).total_seconds() / 60
   
   record = VehicleRealtime(
      wo_id = wo_info.wo_id,
      vehicle_id = data['vehicle_id'],
      ownership = wo_info.ownership,
      ship_to = wo_info.ship_to,
      eta = wo_info.eta,
      status = get_status(data),
      order_datetime = wo_info.order_datetime,
      process_datetime = data['process_datetime'],
      latitude = data['latitude'],
      longitude = data['longitude'],
      speed = data['speed'],
      duration = duration_min,
      overspeed = is_overspeed(data['speed']),
      is_late = is_late(duration_min, eta)
   )
   with get_session() as session:
      session.merge(record)
      session.commit()
      # session.close()

   print("Recorded vehicle info:", record.vehicle_id)

# Configure consumer
consumer = AvroConsumer({
   "bootstrap.servers": "localhost:9092",
   "group.id"         : "vehicle_realtime",
   "schema.registry.url": "http://localhost:8081"
})

# Subscribe to topic
consumer.subscribe(["gps_data"])
try:
   while True:
         msg = consumer.poll(timeout=1)
         if msg is None:
            continue
         if msg.error():
            print("Error: ", msg.error())
         update_vehicle_realtime(msg)
         
finally:
   consumer.close()
