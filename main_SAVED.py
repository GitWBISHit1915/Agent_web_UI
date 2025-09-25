from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel  
from typing import Optional
from controllers.clientcontact_controller import Router, delete_client_contact
from models.building_model import Building
from schemas.building_schema import BuildingCreate, BuildingUpdate, BuildingInDB
from models.entity_model import Entity
from schemas.entity_schema import EntityCreate, EntityUpdate, EntityInDB
from models.clientcontact_model import ClientContact
from schemas.clientcontact_schema import ClientContactBase, ClientContactCreate, ClientContactInDB, ClientContactUpdate
import httpx

DATABASE_URL = 'mssql+pyodbc://wbis_api:a7#J!q9P%bZt$r2d@JOHN\\SQLEXPRESS01/wbis_core?driver=ODBC+Driver+17+for+SQL+Server'


AIRTABLE_BASE_ID="appK6lkMaeCNpBAiY"
AIRTABLE_API_KEY="patWCppXtHNjVDsFR.5490c1dfa5f1d80e1c8930927cee60b5b16063a50df7d2b30c6ac6394eb09dca"
AIRTABLE_TABLE_ID="tbllK1Negu5zcxHXN"

AIRTABLE_API_URL="https://api.airtable.com/v0/appK6lkMaeCNpBAiY/"

def get_airtable_url(table_name: str) -> str:
    return f'{AIRTABLE_API_URL}{table_name}'

buildings_url = get_airtable_url('airtable_Building')
#entites_url = get_airtable_url('airtable_Entity')
#enter other urls here as we go. ('airtable name')
#....
#....

print(buildings_url)
#print(entity_url)
#....
#....

engine = create_engine(DATABASE_URL)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

app = FastAPI()

# Add CORS middleware if necessary
# ...

Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/sync_buildings_to_airtable/")
def sync_buildings_to_airtable(db: Session = Depends(get_db)):
    buildings = db.query(Building).all()
    for building in buildings:
        building_payload = {
            "fields": {
                "building_id": building.building_id,
                "mortgagee_id": building.mortgagee_id,
                "Address Normalized": building.address_normalized,
                "bld_number": building.bld_number,
                "owner_occupied": building.owner_occupied,
                "street_address": building.street_address,
                "city": building.city,
                "state": building.state,
                "zip_code": building.zip_code,
                "county": building.county,
                "units": building.units,
                "construction_code": building.construction_code,
                "year_built": building.year_built,
                "stories": building.stories,
                "square_feet": building.square_feet,
                "desired_building_coverage": building.desired_building_coverage,
                "fire_alarm": building.fire_alarm,
                "sprinkler_system": building.sprinkler_system,
                "roof_year_updated": building.roof_year_updated,
                "plumbing_year_updated": building.plumbing_year_updated,
                "electrical_year_updated": building.electrical_year_updated,
                "hvac_year_updated": building.hvac_year_updated,
                "entity_id": building.entity_id,

            }
            
        }
        #print(f"Syncing Building ID {building.building_id} with payload: {building_payload}")
        response = httpx.post(
            f"{AIRTABLE_API_URL}airtable_BUilding",
            headers={"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"},
            json=building_payload
        )
        print(response.text)
        if response.status_code != 200:
            print(f"Failed to sync building ID {building.building_id}: {response.json()}")
            print(f"Failed to sync building ID {building.building_id}")

    return {"message": "Building synced with Airtable"}


@app.get("/fetch_buildings_from_airtable/")
def fetch_buildings_from_airtable(db: Session = Depends(get_db)):
    response = httpx.get(
        f"{AIRTABLE_API_URL}airtable_Building",
        headers={"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    )
    if response.status_code == 200:
        data = response.json().get('records', [])
        for record in data:
            fields = record['fields']
            building = db.query(Building).filter(Building.address_normalized == fields.get('Address Normalized')).first()
            if building:
                building.mortgagee_id = fields.get('mortgagee_id', building.mortgagee_id)
                building.bld_number = fields.get('bld_number', building.bld_number)
                building.owner_occupied = fields.get('owner_occupied', building.owner_occupied)
                building.street_address = fields.get('street_address', building.street_address)
                building.city = fields.get('city', building.city)
                building.state = fields.get('state', building.state)
                building.zip_code = fields.get('zip_ode', building.zip_code)
                building.county = fields.get('county'), building.county
                building.units = fields.get('units', building.units)
                building.construction_code = fields.get('construction_code', building.construction_code)
                building.year_built = fields.get('year_built', building.year_built)
                building.stories = fields.get('stories', building.stories)
                building.square_feet = fields.get('square_feet', building.square_feet)
                building.desired_building_coverage = fields.get('desired_building_covereage', building.desired_building_coverage)
                building.fire_alarm = fields.get('fire_alarm', building.fire_alarm)
                building.sprinkler_system = fields.get('sprinkler_system', building.sprinkler_system)
                building.roof_year_updated = fields.get('roof_year_updated', building.roof_year_updated)
                building.plumbing_year_updated = fields.get('plumbing_year_updated', building.plumbing_year_updated)
                building.electrical_year_updated = fields.get('electrical_year_updated', building.electrical_year_updated)
                building.hvac_year_updated = fields.get('hvac_year_updated', building.hvac_year_updated)
                building.entity_id = fields.get('entity_id', building.entity_id)
            else:
                new_building = Building(
                    mortgagee_id = fields.get('mortgagee_id'),
                    bld_number = fields.get('bld_number'),
                    address_normalized = fields.get('address_normalized'),
                    owner_occupied = fields.get('owner_occupied'),
                    street_address = fields.get('street_address'),
                    city = fields.get('city'),
                    state = fields.get('state'),
                    zip = fields.get('zip Code'),
                    county = fields.get('county'),
                    units = fields.get('units'),
                    construction_code = fields.get('construction_code'),
                    year_built = fields.get('year_built'),
                    stories = fields.get('stories'),
                    square_feet = fields.get('square_feet'),
                    desired_building_coverage = fields.get('desired_building_covereage'),
                    fire_alarm = fields.get('fire_alarm'),
                    sprinkler_system = fields.get('sprinkler_system'),
                    roof_year_update = fields.get('roof_year_update'),
                    plumbing_year_update = fields.get('plumbing_year_update'),
                    electrical_year_update = fields.get('electrical_year_update'),
                    hvac_year_electrical = fields.get('hvac_year_electrical'),
                    entity_id = fields.get('entity_id'),
            )
            db.add(new_building)
        db.commit()
    return {"message": "Fetch complete"}

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/buildings/", response_model=BuildingInDB)
def create_building(building: BuildingCreate, db: Session = Depends(get_db)):
    db_building = Building(**building.dict())
    db.add(db_building)
    db.commit()
    db.refresh(db_building)
    return db_building

@app.get("/buildings/{building_id}", response_model=BuildingInDB)
def read_building(building_id: int, db: Session = Depends(get_db)):
    building = db.query(Building).filter(Building.building_id == building_id).first()
    if building is None:
        raise HTTPException(status_code=404, detail="Building not found")
    return building

@app.put("/buildings/{building_id}", response_model=BuildingInDB)
def update_building(building_id: int, building: BuildingUpdate, db: Session = Depends(get_db)):
    db_building = db.query(Building).filter(Building.building_id == building_id).first()
    if db_building is None:
        raise HTTPException(status_code=404, detail="Building not found")
    for key, value in building.dict(exclude_unset=True).items():
        setattr(db_building, key, value)
    db.commit()
    return db_building

@app.delete("/buildings/{building_id}", response_model=BuildingInDB)
def delete_building(building_id: int, db: Session = Depends(get_db)):
    db_building = db.query(Building).filter(Building.building_id == building_id).first()
    if db_building is None:
        raise HTTPException(status_code=404, detail="Building not found")
    db.delete(db_building)
    db.commit()
    return db_building

@app.post("/entity/", response_model=EntityInDB)
def create_entity(entity: EntityCreate, db: Session = Depends(get_db)):
    db_entity = Entity(**entity.dict())
    db.add(db_entity)
    db.commit()
    db.refresh(db_entity)
    return db_entity

@app.get("/entity/{entity_id}", response_model=EntityInDB)
def read_entity(entity_id: int, db: Session = Depends(get_db)):
    db_entity = db.query(Entity).filter(Entity.entity_id == entity_id).first()
    if db_entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return db_entity

@app.put("/entity/{entity_id}", response_model=EntityInDB)
def update_entity(entity_id: int, entity: EntityUpdate, db: Session = Depends(get_db)):
    db_entity = db.query(Entity).filter(Entity.entity_id == entity_id).first()
    if db_entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    for key, value in entity.dict(exclude_unset=True).items():
        setattr(db_entity, key, value)
    db.commit()
    return db_entity

@app.delete("/entity/{entity_id}", response_model=EntityInDB)
def delete_entity(entity_id: int, db: Session = Depends(get_db)):
    db_entity = db.query(Entity).filter(Entity.entity_id == entity_id).first()
    if db_entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    db.delete(db_entity)
    db.commit()
    return db_entity

@app.post("/clientcontacts/", response_model=ClientContactInDB)
def create_client_contact(client_contact: ClientContactCreate, db: Session = Depends(get_db)):
    db_client_contact = ClientContact(**client_contact.dict())
    db.add(db_client_contact)
    db.commit()
    db.refresh(db_client_contact)
    return db_client_contact

@app.get("/clientcontacts/{client_contact_id}", response_model=ClientContactInDB)
def read_client_contact (client_contact_id: int, db: Session = Depends(get_db)):
    db_client_contact = db.query(ClientContact).filter(ClientContact.client_contact_id == client_contact_id).first()
    if db_client_contact is None:
        raise HTTPException(status_code=404, detail="ClientContact not found")
    return db_client_contact

@app.put("/clientcontacts/{client_contact_id}", response_model=ClientContactInDB)
def update_client_contact(client_contact_id: int, client_contact: ClientContactUpdate, db: Session = Depends(get_db)):
    db_client_contact = db.query(ClientContact).filter(ClientContact.client_contact_id == client_contact_id).first()
    if db_client_contact is None:
        raise HTTPException(status_code=404, detail="ClientContact not found")
    for key, value in client_contact.dict(exclude_unset=True).items():
        setattr(db_client_contact, key, value)
    db.commit()
    return db_client_contact

@app.delete("/clientcontacts/{client_contact_id}", response_model=ClientContactInDB)
def delete_client_contact(client_contact_id: int, db: Session = Depends(get_db)):
    db_client_contact = db.query(ClientContact).filter(ClientContact.client_contact_id == client_contact_id).first()
    if db_client_contact is None:
        raise HTTPException(status_code=404, detail="ClientContact not found")
    db.delete(db_client_contact)
    db.commit()
    return db_client_contact


