import datetime

from pydantic import BaseModel


class Context(BaseModel):
    filename:str
    runtime: datetime = datetime.datetime.now()
