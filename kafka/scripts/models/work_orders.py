from datetime import datetime
from sqlmodel import SQLModel, Field

class eta(SQLModel, table=True):
    __tablename__ = "ETA"
    ownership: str = Field(primary_key=True)
    ship_to: str = Field(primary_key=True)
    eta: int

class WO_History(SQLModel, table=True):
    __tablename__ = "WO_History"
    wo_id: str = Field(primary_key=True)
    vehicle_id: str
    ownership: str
    ship_to: str
    order_datetime: datetime

class Work_Orders(SQLModel, table=True):
    __tablename__ = "Work_Orders"
    wo_id: str = Field(primary_key=True)
    vehicle_id: str
    ownership: str
    ship_to: str
    order_datetime: datetime
    eta: int

