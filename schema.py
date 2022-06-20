from pydantic import BaseModel
from typing import List, Dict
class JobSchema(BaseModel):
    name:str
    location:str
    args:List
    kwargs:Dict

