from datetime import datetime
from sqlmodel import SQLModel, Field

class GpsHistory(SQLModel, table=True):
    __tablename__ = "GPS_History"
    id: int = Field(primary_key=True)
    vehicle_id: str
    process_datetime: datetime
    speed: int

class Work_Orders(SQLModel, table=True):
    __tablename__ = "Work_Orders"
    wo_id: str = Field(primary_key=True)
    vehicle_id: str
    ownership: str
    ship_to: str
    order_datetime: datetime
    eta: int

class VehicleRealtime(SQLModel, table=True):
    __tablename__ = "Vehicle_Realtime"
    vehicle_id: str = Field(primary_key=True)
    wo_id: str
    ownership: str
    ship_to: str
    eta: int
    order_datetime: datetime
    process_datetime: datetime
    latitude: float
    longitude: float
    speed: int
    overspeed: int
    duration: int
    is_late: int