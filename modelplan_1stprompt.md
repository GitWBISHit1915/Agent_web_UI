Let's go step by step to create a functional FastAPI application using modular files for your models and schemas. We will start by creating the model files based on your SQL schema. Here's how to proceed:


Step 1: Create the Models


Create a models directory:



Inside your project directory, create a folder named models.


bash hljs
Copy
    
    mkdir models


Create __init__.py:



Inside the models directory, create an empty __init__.py file to make it a package.


bash hljs
Copy
    
    touch models/__init__.py


Create the Building Model:



Create a file named building.py in the models directory.


bash hljs
Copy
    
    touch models/building.py


Open models/building.py and define the Building model based on your SQL schema. Here’s a basic template you can adapt:


python hljs
Copy
    
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


Create Additional Models:



Repeat the above steps to create additional model files (e.g., entity.py, mortgagee.py, etc.) based on the structure of your database schema provided in the SQL file. Here is a simple template for each:


bash hljs
Copy
    
    touch models/entity.py  # Entity model
    touch models/mortgagee.py  # Mortgagee model


Populate these files with the necessary SQLAlchemy model definitions similar to how we did for building.py.




Step 2: Create the Schemas


Create a schemas directory:



Inside your project directory, create another folder named schemas.


bash hljs
Copy
    
    mkdir schemas


Create __init__.py:



Inside the schemas directory, create an empty __init__.py file.


bash hljs
Copy
    
    touch schemas/__init__.py


Create the Building Schema:



Create a file named building.py in the schemas directory.


bash hljs
Copy
    
    touch schemas/building.py


Open schemas/building.py and define the Building schema:


python hljs
Copy
    
    from pydantic import BaseModel
    from typing import Optional

    class BuildingBase(BaseModel):
        mortgagee_id: Optional[int] = None
        address_normalized: str
        bld_number: int
        owner_occupied: bool
        street_address: Optional[str] = None
        city: Optional[str] = None
        state: Optional[str] = None
        zip_code: Optional[str] = None
        county: Optional[str] = None
        units: Optional[int] = None
        construction_code: int
        year_built: Optional[int] = None
        stories: Optional[int] = None
        square_feet: Optional[int] = None
        desired_building_coverage: Optional[float] = None
        fire_alarm: bool
        sprinkler_system: bool
        roof_year_updated: Optional[int] = None
        plumbing_year_updated: Optional[int] = None
        electrical_year_updated: Optional[int] = None
        hvac_year_updated: Optional[int] = None
        entity_id: Optional[int] = None

    class BuildingCreate(BuildingBase):
        pass

    class BuildingUpdate(BuildingBase):
        pass

    class BuildingInDB(BuildingBase):
        building_id: int


Create Additional Schemas:



Repeat the process for other models (e.g., create entity.py, mortgagee.py, etc.) in the schemas directory with the necessary Pydantic class definitions.




Step 3: Update main.py


Edit main.py:



You need to update the main.py file to import your models and schemas and define your API routes.


python hljs
Copy
   
       from fastapi import FastAPI, Depends, HTTPException
        from sqlalchemy.orm import Session
        from database import SessionLocal
        from models.building import Building
        from schemas.building import BuildingCreate, BuildingInDB, BuildingUpdate
        import crud  # This will be created next

    app = FastAPI()

    def get_db():
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    @app.post("/buildings/", response_model=BuildingInDB)
    def create_building(building: BuildingCreate, db: Session = Depends(get_db)):
        return crud.create_building(db=db, building=building)

    # Add other endpoints for read, update, and delete.



Conclusion for Step 1 and 2


You have now set up the necessary model and schema files for Building. 

Complete the additional models and schemas based on your SQL schema in a similar manner.

Once you finish with these, let me know, and we can proceed to set up the CRUD operations and update the main.py file.

