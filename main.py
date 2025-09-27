from dataclasses import MISSING
from logging import PlaceHolder
from fastapi import Body, FastAPI, Depends, HTTPException, Header
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel  
import pyodbc
from typing import Optional, Required
from controllers.clientcontact_controller import Router, delete_client_contact
from models.building_model import Building
from schemas.building_schema import BuildingCreate, BuildingUpdate, BuildingInDB
from models.entity_model import Entity
from schemas.entity_schema import EntityCreate, EntityUpdate, EntityInDB
from models.clientcontact_model import ClientContact
from schemas.clientcontact_schema import ClientContactBase, ClientContactCreate, ClientContactInDB, ClientContactUpdate
import httpx
from shutil import get_terminal_size
COLS = get_terminal_size(fallback=(80,24)).columns


#insert env

REQUIRED_BUILDING_FIELDS = [
    "bld_number", "owner_occupied",
    "street_address", "city", "state", "zip_code", "county",
    "construction_code", "fire_alarm", "sprinkler_system",
]

def _is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "")

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

CONN_STR = "DSN=wbis_core_dsn;Trusted_Connection=Yes;"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def normalize_address(street: str, city: str, state: str, zip_code: str) -> str:
    to_s = lambda x: str(x or "").strip()
    s = lambda x: to_s(x)
    z_raw = to_s(zip_code)
    z = z_raw[:5]
    parts = [s(street), s(city), s(state)]
    core = ", ".join(p for p in parts if p)
    return (core + (f" {z}" if z else "")).strip()

def find_airtable_record_id(building:Building) -> Optional[str]:
    """Searches Airtable for a cord that matches the given building and returns its record ID."""
    response = httpx.get(
        f"{AIRTABLE_API_URL}airtable_Building",
        headers={"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    )
    if response.status_code == 200:
        records = response.json().get('records', [])
        for record in records:
            if (record['fields'].get('Address Normalized') == building.address_normalized and 
                    record['fields'].get('bld_number') == building.bld_number):
                return record['id']
    return None

@app.post("/airtable/buildings/ingest")
def ingest_building(payload: dict = Body(...)): # "C" in Crud (From Aitable Perspective)
    """
    Accepts Airtable-like payload:
    either { "fields": { ... } }  OR  the fields dict directly.
    Inserts a new row into dbo.Building. Minimal logic; no ORM.
    """
    fields = payload.get("fields", payload)
    if fields is None and isinstance(payload.get("records"), list) and payload["records"]:
        fields = payload["records"][0].get("fields", {})
    if fields is None:
        fields = payload

    # Accept Airtable names with spaces and map to our snake_case keys
    aliases = {
        "Address Normalized": "address_normalized",
        "Bld#": "bld_number",
        "Owner Occupied": "owner_occupied",
        "Street Address": "street_address",
        "Zip": "zip_code",
        "Square Feet": "square_feet",
        "Year Built": "year_built",
        "Desired Building Coverage": "desired_building_coverage",
        "Fire Alarm": "fire_alarm",
        "Sprinkler System": "sprinkler_system",
        "City": "city",
        "State": "state",
        "County": "county",
        "Units": "units",
        "Stories": "stories",
    }
    for old, new in aliases.items():
        if old in fields and new not in fields:
            fields[new] = fields[old]
    
    norm = {}
    for k, v in fields.items():
        kk = k.strip()
        snake = kk.lower().replace(" ", "_")

        if kk == "Bld#":
            norm["bld_number"] = v
            continue
        if kk == "Zip":
            norm["zip_code"] = v
            continue

        norm[snake] = v

    fields = norm

    missing = [k for k in REQUIRED_BUILDING_FIELDS if _is_blank(fields.get(k))]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing required: {', '.join(missing)}")

    if not fields.get("address_normalized") or (isinstance(fields.get("address_normalized"), str) and fields["address_normalized"].strip() == ""):
        fields["address_normalized"] = normalize_address(
            fields.get("street_address") or fields.get("Street Address"),
            fields.get("city"),
            fields.get("state"),
            fields.get("zip_code") or fields.get("Zip"),
        )

    addr = fields.get("address_normalized") or fields.get("Address Normalized")
    bldn = fields.get("bld_number") or fields.get("Bld#") or 1
    
    print("DEBUG keys in fields:", list(fields.keys()))
    if not fields.get("address_normalized"):
        raise HTTPException(status_code=400, detail="address_normalized is required")
    if fields.get("construction_code") is None:
        raise HTTPException(status_code=400, detail="construction_code is  required")

    colmap = {
            "mortgagee_id": "mortgagee_id",
            "address_normalized": "Address Normalized",
            "bld_number": "Bld#",
            "owner_occupied": "Owner Occupied",
            "street_address": "Street Address",
            "city": "City",
            "state": "State",
            "zip_code": "Zip",
            "county": "County",
            "units": "Units",
            "construction_code": "construction_code",
            "year_built": "Year Built",
            "stories": "Stories",
            "square_feet": "Square Feet",
            "desired_building_coverage": "Desired Building Coverage",
            "fire_alarm": "Fire Alarm",
            "sprinkler_system": "Sprinkler System",
            "roof_year_updated": "roof_year_updated",
            "plumbing_year_updated": "plumbing_year_updated",
            "electrical_year_updated": "electrical_year_updated",
            "hvac_year_updated": "hvac_year_updated",
            "entity_id": "entity_id",
  
        }

    cols = []
    vals = []
    for api_key, sql_col in colmap.items():
        if api_key in fields:
            val = fields[api_key]

            if api_key in ("owner_occupied", "fire_alarm", "sprinkler_system") and val is not None:
                val = int(bool(val))
            cols.append(sql_col)
            vals.append(val)

    if "Address Normalized" not in cols:
        raise HTTPException(status_code=400, detail="address_normalized is required")
    if "construction_code" not in cols:
        raise HTTPException(status_code=400, detail="construction_code is required")

    placeholders = ", ".join(["?"] * len(cols))
    collist = ", ".join(f"[{c}]" for c in cols)  

    insert_sql = f"INSERT INTO dbo.Building ({collist}) VALUES ({placeholders});"
    scope_sql = "SELECT CAST(SCOPE_IDENTITY() AS INT);"

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(insert_sql, vals)
        cur.execute("SELECT CAST(SCOPE_IDENTITY() AS INT);")
        row = cur.fetchone()
        new_id = int(row[0]) if row and row[0] is not None else None

        if new_id is None:
            cur.execute(
                "SELECT building_id FROM dbo.building WHERE [Address Normalized] = ? AND [Bld#] = ?",
                (addr, bldn),
            )
            r = cur.fetchone()
            new_id = int(r[0]) if r else None
        conn.commit()
        return {
            "status": "ok", 
            "building_id": new_id,
            "address_normalized": fields["address_normalized"],
        }
    
    except pyodbc.Error as e:

        msg = str(e)
        if "2627" in msg or "2601" in msg:
            cur.execute(
                "SELECT building_id FROM dbo.Building WHERE [Address Normalized] = ? AND [Bld#] = ?",
                (addr, bldn),
            )
            r = cur.fetchone()
            new_id = int(r[0]) if r else None
            conn.commit()
            return {
                "status": "ok", 
                "building_id": new_id,
                "address_normalized": fields["address_normalized"],
            }
            
        raise HTTPException(status_code=500, detail=f"DB error: {msg}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.post("/sync_buildings_to_airtable/") # Syncs all records in SQL DB to Airtable
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
        existing_record_id = find_airtable_record_id(building)
        #code block below needs tested - one succesful test on 09/24/2025 changing buildin_id=18 year built 1954 to 1964 (pt 2)

        if existing_record_id:
            response = httpx.patch(
                f"{AIRTABLE_API_URL}airtable_Building/{existing_record_id}",
                headers={"Authorization": f"Bearer {AIRTABLE_API_KEY}", "Content-Type": "application/json"},
                json=building_payload
            )
        else:
            response = httpx.post(
                f"{AIRTABLE_API_URL}airtable_Building",
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

@app.post("/airtable/buildings/update")
def update_building_from_airtable(payload: dict = Body(...)):
    """
    Minimal, column-selective UPDATE for dbo.Building.
    Accepts:
      - { "fields": { ... } }
      - { "records": [ { "fields": { ... } } ] }
      - raw fields object
    Requires: building_id in the payload.
    """
    fields = payload.get("fields")
    if fields is None and isinstance(payload.get("records"), list) and payload["records"]:
        fields = payload["records"][0].get("fields", {})
    if fields is None:
        fields = payload

    norm = {}
    for k, v in (fields or {}).items():
        kk = (k or "").strip()
        if kk == "Bld#": norm["bld_number"] = v; continue
        if kk == "Zip": norm["zip_code"] = v; continue
        norm[kk.lower().replace(" ", "_")] =v
    fields = norm

    bld_id = fields.get("building_id")
    if bld_id in (None, ""):
        raise HTTPException(status_code=400, detail="building_id is required for updates")

    colmap = {
        "mortgagee_id": "mortgagee_id",
        "address_normalized": "Address Normalized",
        "bld_number": "Bld#",
        "owner_occupied": "Owner Occupied",
        "street_address": "Street Address",
        "city": "City",
        "state": "State",
        "zip_code": "Zip",
        "county": "County",
        "units": "Units",
        "construction_code": "construction_code",
        "year_built": "Year Built",
        "stories": "Stories",
        "square_feet": "Square Feet",
        "desired_building_coverage": "Desired Building Coverage",
        "fire_alarm": "Fire Alarm",
        "sprinkler_system": "Sprinkler System",
        "roof_year_updated": "roof_year_updated",
        "plumbing_year_updated": "plumbing_year_updated",
        "electrical_year_updated": "electrical_year_updated",
        "hvac_year_updated": "hvac_year_updated",
        "entity_id": "entity_id",
}

    set_cols, set_vals = [], []
    for api_key, sql_col in colmap.items():
        if api_key in fields:
            val = fields[api_key]
            if api_key in ("owner_occupied", "fire_alarm", "sprinkler_system") and val is not None:
                val = int(bool(val))
            set_cols.append(f"[{sql_col}] = ?")
            set_vals.append(val)

    if not set_cols:
        return {"status": "ok", "updated": 0, "building_id": int(bld_id)}

    sql = "UPDATE dbo.Building SET " + ", ".join(set_cols) + " WHERE building_id = ?"
    set_vals.append(int(bld_id))

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, set_vals)
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "updated": rows, "building_id": int(bld_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

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


