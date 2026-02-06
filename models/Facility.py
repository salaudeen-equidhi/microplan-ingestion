from pydantic import BaseModel


class Facility(BaseModel):
    facility_code:str
    facility_name: str
    contact:str
    email:str
    is_permanent: bool
    facility_type: str = "Warehouse"
    administrative_area: str
    boundary_code: str
