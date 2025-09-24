from sqlalchemy import Column, Integer, String, Boolean
from database import Base

class ClientContact(Base):
    __tablename__ = 'ClientContact'
    
    client_contact_id = Column('ClientContact_Id', Integer, primary_key=True, autoincrement=True)
    first_name = Column('First Name', String(255), nullable=False)
    last_name = Column('Last Name', String(255), nullable=False)
    mailing_address = Column('Mailing Address', String(255), nullable=True)
    physical_address = Column('Physical Address', String(200), nullable=True)
    phone = Column('Phone', String(25), nullable=True)
    email = Column('Email', String(254), nullable=True)
    is_primary = Column(Integer, nullable=False)
    parent_contact_id = Column(Integer, nullable=True)



