from datetime import date
from pydantic import BaseModel
from typing import Optional

class EntityBase(BaseModel):
    legal_name: str
    state_registration: Optional[str] = None
    entity_start_date: Optional[date] = None  # Depending on your requirements, you might want to use `datetime.date`
    fein: Optional[str] = None
    sos_url: Optional[str] = None

class EntityCreate(EntityBase):
    pass  # Additional validation can be added here if necessary

class EntityUpdate(BaseModel):
    legal_name: Optional[str] = None
    state_registration: Optional[str] = None
    entity_start_date: Optional[date] = None
    fein: Optional[str] = None
    sos_url: Optional[str] = None

class EntityInDB(EntityBase):
    entity_id: int

    class Config:
        orm_mode = True  # Allows reading data as dict.
