from sqlalchemy import Column, Integer, String, Boolean, DECIMAL
from database import Base

class Building(Base):
    __tablename__ = 'Building'
    
    building_id = Column(Integer, primary_key=True, autoincrement=True)
    mortgagee_id = Column(Integer, nullable=True)
    address_normalized = Column('Address Normalized', String(200), nullable=False)
    bld_number = Column('Bld#', Integer, nullable=False)
    owner_occupied = Column('Owner Occupied', Boolean, nullable=False)
    street_address = Column('Street Address', String(200), nullable=True)
    city = Column('City', String(100), nullable=True)
    state = Column('State', String(2), nullable=True)
    zip_code = Column('Zip', String(10), nullable=True)
    county = Column('County', String(100), nullable=True)
    units = Column('Units', Integer, nullable=True)
    construction_code = Column(Integer, nullable=False)
    year_built = Column('Year Built', Integer, nullable=True)
    stories = Column('Stories', Integer, nullable=True)
    square_feet = Column('Square Feet', Integer, nullable=True)
    desired_building_coverage = Column('Desired Building Coverage', DECIMAL(19, 0), nullable=True)
    fire_alarm = Column('Fire Alarm', Boolean, nullable=False)
    sprinkler_system = Column('Sprinkler System', Boolean, nullable=False)
    roof_year_updated = Column(Integer, nullable=True)
    plumbing_year_updated = Column(Integer, nullable=True)
    electrical_year_updated = Column(Integer, nullable=True)
    hvac_year_updated = Column(Integer, nullable=True)
    entity_id = Column(Integer, nullable=True)
