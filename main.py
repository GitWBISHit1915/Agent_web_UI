from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DECIMAL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel  
from typing import Optional
from controllers.clientcontact_controller import delete_client_contact
from models.building_model import Building
from schemas.building_schema import BuildingCreate, BuildingUpdate, BuildingInDB
from models.entity_model import Entity
from schemas.entity_schema import EntityCreate, EntityUpdate, EntityInDB
from models.clientcontact_model import ClientContact
from schemas.clientcontact_schema import ClientContactBase, ClientContactCreate, ClientContactInDB, ClientContactUpdate
import httpx

DATABASE_URL = 'mssql+pyodbc://wbis_api:a7#J!q9P%bZt$r2d@JOHN\\SQLEXPRESS01/wbis_core?driver=ODBC+Driver+17+for+SQL+Server'

AIRTABLE_BASE_ID = "appK6lkMaeCNpBAiY"
AIRTABLE_API_KEY = "patWCppXtHNjVDsFR.5490c1dfa5f1d80e1c8930927cee60b5b16063a50df7d2b30c6ac6394eb09dca"
AIRTABLE_API_URL = f'https://api.airtable.com/v0/appK6lkMaeCNpBAiY/'


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


