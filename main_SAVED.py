from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DECIMAL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel  
from typing import Optional

# Database connection string
DATABASE_URL = 'mssql+pyodbc://wbis_api:a7#J!q9P%bZt$r2d@JOHN\\SQLEXPRESS01/wbis_core?driver=ODBC+Driver+17+for+SQL+Server'

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Session local factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base model for declarative classes
Base = declarative_base()

# FastAPI application instance
app = FastAPI()

# Define the Building model
class Building(Base):
    __tablename__ = 'Building'

    building_id = Column(Integer, primary_key=True, autoincrement=True)
    mortgagee_id = Column(Integer, nullable=True)
    address_normalized = Column('Address Normalized', String(200), nullable=False)
    bld_number = Column('Bld#', Integer, nullable=False)
    owner_occupied = Column('Owner Occupied', Boolean, nullable=False)  # Changed Bit to Boolean
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
    fire_alarm = Column('Fire Alarm', Boolean, nullable=False)  # Changed Bit to Boolean
    sprinkler_system = Column('Sprinkler System', Boolean, nullable=False)  # Changed Bit to Boolean
    roof_year_updated = Column(Integer, nullable=True)
    plumbing_year_updated = Column(Integer, nullable=True)
    electrical_year_updated = Column(Integer, nullable=True)
    hvac_year_updated = Column(Integer, nullable=True)
    entity_id = Column(Integer, nullable=True)

# Base model for Pydantic
class BuildingBase(BaseModel):
    """Pydantic model to validate incoming building data"""
    building_id: Optional[int] = None        # Primary key, optional during creation
    mortgagee_id: Optional[int] = None        # Foreign key, optional since it's nullable
    address_normalized: str
    bld_number: int
    owner_occupied: bool
    street_address: Optional[str] = None      # Made Optional
    city: Optional[str] = None                # Made Optional
    state: Optional[str] = None               # Made Optional
    zip_code: Optional[str] = None            # Made Optional
    county: Optional[str] = None              # Made Optional
    units: Optional[int] = None               # Made Optional
    construction_code: int
    year_built: Optional[int] = None          # Made Optional
    stories: Optional[int] = None             # Made Optional
    square_feet: Optional[int] = None         # Made Optional
    desired_building_coverage: Optional[float] = None  # Made Optional
    fire_alarm: bool
    sprinkler_system: bool
    roof_year_updated: Optional[int] = None    # Made Optional
    plumbing_year_updated: Optional[int] = None # Made Optional
    electrical_year_updated: Optional[int] = None # Made Optional
    hvac_year_updated: Optional[int] = None     # Made Optional
    entity_id: Optional[int] = None             # Made Optional

class BuildingUpdate(BaseModel):
    """Pydantic model to validate incoming building update data"""
    mortgagee_id: Optional[int] = None        
    address_normalized: Optional[str] = None    
    bld_number: Optional[int] = None            
    owner_occupied: Optional[bool] = None       
    street_address: Optional[str] = None        
    city: Optional[str] = None                  
    state: Optional[str] = None                 
    zip_code: Optional[str] = None              
    county: Optional[str] = None                
    units: Optional[int] = None                 
    construction_code: Optional[int] = None     
    year_built: Optional[int] = None            
    stories: Optional[int] = None              
    square_feet: Optional[int] = None           
    desired_building_coverage: Optional[float] = None  
    fire_alarm: Optional[bool] = None           
    sprinkler_system: Optional[bool] = None    
    roof_year_updated: Optional[int] = None      
    plumbing_year_updated: Optional[int] = None  
    electrical_year_updated: Optional[int] = None 
    hvac_year_updated: Optional[int] = None       
    entity_id: Optional[int] = None               

# Model for creating a new building
class BuildingCreate(BuildingBase):
    pass

# Model for returning building data from the database
class BuildingInDB(BuildingBase):
    building_id: int

    class Config:
        from_attributes = True  # Change 'orm_mode' to 'from_attributes'

# Create tables in the database
Base.metadata.create_all(bind=engine)

# Dependency for database session management
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# API endpoint to get all buildings
@app.get("/buildings/")
def read_buildings(skip: int = 0, limit: int = 10, db: Session = Depends(get_db)):
    buildings = db.query(Building).order_by(Building.building_id).offset(skip).limit(limit).all()
    return buildings

# API endpoint to create a new building
@app.post("/buildings/")
def create_building(building: BuildingCreate, db: Session = Depends(get_db)):
    db_building = Building(
        mortgagee_id=building.mortgagee_id,
        fire_alarm=1 if building.fire_alarm else 0,  # Convert boolean to integer
        sprinkler_system=1 if building.sprinkler_system else 0,
        # Map all other fields similarly
        address_normalized=building.address_normalized,
        bld_number=building.bld_number,
        owner_occupied=building.owner_occupied,
        street_address=building.street_address,
        city=building.city,
        state=building.state,
        zip_code=building.zip_code,
        county=building.county,
        units=building.units,
        construction_code=building.construction_code,
        year_built=building.year_built,
        stories=building.stories,
        square_feet=building.square_feet,
        desired_building_coverage=building.desired_building_coverage,
        roof_year_updated=building.roof_year_updated,
        plumbing_year_updated=building.plumbing_year_updated,
        electrical_year_updated=building.electrical_year_updated,
        hvac_year_updated=building.hvac_year_updated,
        entity_id=building.entity_id
    )
    
    db.add(db_building)
    db.commit()
    db.refresh(db_building)
    return db_building

# API endpoint to get a single building by ID
@app.get("/buildings/{building_id}")
def read_building(building_id: int, db: Session = Depends(get_db)):
    building = db.query(Building).filter(Building.building_id == building_id).first()
    if not building:
        raise HTTPException(status_code=404, detail="Building not found")
    return building

@app.put("/buildings/{building_id}")
def update_building(building_id: int, building_data: BuildingUpdate, db: Session = Depends(get_db)):
    building = db.query(Building).filter(Building.building_id == building_id).first()
    if not building:
        raise HTTPException(status_code=404, detail="Building not found")

    # Update the fields conditionally
    for key, value in building_data.dict(exclude_unset=True).items():
        # Only update if value is not None (or not blank if you implement such a check)
        if value is not None:  # Check the value before updating
            setattr(building, key, value)

    db.commit()
    db.refresh(building)
    return building
