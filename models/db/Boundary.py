import datetime

from sqlalchemy import Column, String, Integer, DateTime

import constants
from constants import CHECKLIST_TARGETS
from models.db import Base

class Boundary(Base):
    __tablename__ = "egov_microplan_boundaries"

    code = Column(String, primary_key=True)
    name_in_english = Column(String, default=None)
    name = Column(String)
    parent_code = Column(String)
    boundary_type = Column(String)
    boundary_level = Column(Integer)
    campaign_start_date = Column(String, default=None)
    campaign_end_date = Column(String, default=None)
    target_1 = Column(Integer, default=0)
    target_2 = Column(Integer, default=0)
    target_3 = Column(Integer, default=0)
    target_4 = Column(Integer, default=0)
    target_5 = Column(Integer, default=0)
    target_6 = Column(Integer, default=0)
    target_7 = Column(Integer, default=0)
    target_8 = Column(Integer, default=0)
    target_9 = Column(Integer, default=0)
    target_10 = Column(Integer, default=0)
    target_11 = Column(Integer, default=0)
    target_12 = Column(Integer, default=0)
    target_13 = Column(Integer, default=0)
    target_14 = Column(Integer, default=0)
    target_15 = Column(Integer, default=0)
    total_1 = Column(Integer, default=0)
    total_2 = Column(Integer, default=0)
    total_3 = Column(Integer, default=0)
    total_4 = Column(Integer, default=0)
    total_5 = Column(Integer, default=0)
    total_6 = Column(Integer, default=0)
    total_7 = Column(Integer, default=0)
    total_8 = Column(Integer, default=0)
    total_9 = Column(Integer, default=0)
    total_10 = Column(Integer, default=0)
    total_11 = Column(Integer, default=0)
    total_12 = Column(Integer, default=0)
    total_13 = Column(Integer, default=0)
    total_14 = Column(Integer, default=0)
    total_15 = Column(Integer, default=0)
    checklist_target = Column(String, default=CHECKLIST_TARGETS, nullable=False)
    insert_time = Column(DateTime, nullable=False, default=datetime.datetime.now())
    update_time = Column(DateTime, nullable=False, default=datetime.datetime.now(), onupdate=datetime.datetime.now())
    filename = Column(String, default=None, nullable=True)

    def __repr__(self):
        return f"Boundary: {self.code}_{self.name}_{self.boundary_type}"
