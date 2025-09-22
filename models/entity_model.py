from sqlalchemy import Column, Integer, String, Date
from database import Base

class Entity(Base):
    __tablename__ = 'Entity'
    
    entity_id = Column(Integer, primary_key=True, autoincrement=True)  # NOT NULL
    legal_name = Column(String(200), nullable=False)     # NOT NULL
    state_registration = Column('State Registration', String(2), nullable=True)  # NULL
    entity_start_date = Column('Entity Start Date', Date, nullable=True)  # NULL
    fein = Column('FEIN', String(15), nullable=True)  # NULL
    sos_url = Column('sos_url', String(500), nullable=True)  # NULL

