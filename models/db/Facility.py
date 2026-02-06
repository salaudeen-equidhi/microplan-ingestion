import datetime

from sqlalchemy import Column, String, Boolean, Integer, Float, PrimaryKeyConstraint, DateTime


from models.db import Base


class Facility(Base):
    __tablename__ = "egov_microplan_facilities"

    facility_name = Column(String)
    is_permanent = Column(String, default="FALSE")
    facility_type = Column(String)
    boundary_code = Column(String, nullable=True)
    administrative_area = Column(String, default=None, nullable=True)
    address_line_1 = Column(String, default=None, nullable=True)
    address_line_2 = Column(String, default=None, nullable=True)
    landmark = Column(String, default=None, nullable=True)
    city = Column(String, default=None, nullable=True)
    pincode = Column(String, default=None, nullable=True)
    building_name = Column(String, default=None, nullable=True)
    street = Column(String, default=None, nullable=True)
    storage = Column(Integer, default=0, nullable=True)
    latitude = Column(Float, default=None, nullable=True)
    longitude = Column(Float, default=None, nullable=True)
    parent_code = Column(String, default=None, nullable=True)
    uuid = Column(String, default=None, nullable=True)
    target = Column(Integer, default=0, nullable=False)
    insert_time = Column(DateTime, nullable=False, default=datetime.datetime.now())
    update_time = Column(DateTime, nullable=False, default=datetime.datetime.now(), onupdate=datetime.datetime.now())
    filename = Column(String, default=None, nullable=True)


    __table_args__ = (PrimaryKeyConstraint('facility_name', 'boundary_code'),)

    def __repr__(self):
        return f"Facility_{self.facility_name}_{self.boundary_code}"
