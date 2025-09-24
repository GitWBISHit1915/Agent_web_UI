from pydantic import BaseModel
from typing import Optional

class ClientContactBase(BaseModel):
    first_name: str
    last_name: str
    mailing_address: Optional[str] = None
    physical_address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    is_primary: int
    parent_contact_id: Optional[int] = None

class ClientContactCreate(ClientContactBase):
    pass

class ClientContactUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    mailing_address: Optional[str] = None
    physical_address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    is_primary: Optional[int] = None
    parent_contact_id: Optional[int] = None

class ClientContactInDB(ClientContactBase):
    client_contact_id: int

    class Config:
        orm_mode = True
   
