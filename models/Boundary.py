import datetime

from pydantic import BaseModel

import constants
import utils


class Boundary(BaseModel):
    code: str
    name_in_english: str = None
    name: str
    parent_code: str = None
    boundary_type: str
    boundary_level: int
    campaign_start_date: str = datetime.datetime.now().strftime(constants.DATE_FORMAT)
    campaign_end_date: str = datetime.datetime.now().strftime(constants.DATE_FORMAT)
    total_households: int = 0
    targeted_households: int = 0
    total_individuals: int = 0
    targeted_individuals: int = 0
    estimated_bednets: int = 0
