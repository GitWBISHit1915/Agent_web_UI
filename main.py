import os, sys
from dotenv import load_dotenv, find_dotenv, dotenv_values
from dataclasses import MISSING
from logging import PlaceHolder
from fastapi import Body, FastAPI, Depends, HTTPException, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from pydantic import BaseModel  
import pyodbc
from typing import Optional, Required, List
from controllers.clientcontact_controller import Router, delete_client_contact
from models.building_model import Building
from schemas.building_schema import BuildingCreate, BuildingUpdate, BuildingInDB
from models.entity_model import Entity
from schemas.entity_schema import EntityCreate, EntityUpdate, EntityInDB
from models.clientcontact_model import ClientContact
from schemas.clientcontact_schema import ClientContactBase, ClientContactCreate, ClientContactInDB, ClientContactUpdate, IdList
import httpx
from shutil import get_terminal_size
from datetime import datetime, timezone
COLS = get_terminal_size(fallback=(80,24)).columns

###******************###
###       ENV        ###
###******************###

env_path = find_dotenv()
env_loaded = load_dotenv(env_path, override=True, verbose=True, encoding="utf-8-sig")
parsed = dotenv_values(env_path, encoding="utf-8-sig")
print("dotenv path ->", env_path)
print("dotenv keys parsed ->", list(parsed.keys()))
print("Loaded:", env_loaded)
print("Has DATABASE_URL:", "DATABASE_URL" in os.environ)
print("PWD:", os.getcwd())
print("DATABASE_URL len:", len(os.environ.get("DATABASE_URL", "")))

# --- Fail fast on required keys ---
required = ["DATABASE_URL", "AIRTABLE_API_KEY", "AIRTABLE_BASE_ID"]
missing = [k for k in required if not os.environ.get(k)]
if missing:
    print("Missing required env keys:", missing)
    print("find_dotenv path check ->", find_dotenv())
    sys.exit(1)

# --- Assign settings from env ---
DATABASE_URL      = os.environ.get("DATABASE_URL")
AIRTABLE_BASE_ID  = os.environ.get("AIRTABLE_BASE_ID")
AIRTABLE_API_KEY  = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_TABLE_ID = os.environ.get("AIRTABLE_TABLE_ID")
AIRTABLE_API_URL  = os.environ.get("AIRTABLE_API_URL") or (f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/" if AIRTABLE_BASE_ID else None)
CONN_STR          = os.environ.get("CONN_STR")

engine = create_engine(DATABASE_URL)

###******************###
###       ENV        ###
###******************###

#Global Definitions

REQUIRED_BUILDING_FIELDS = [
    "bld_number", "owner_occupied",
    "street_address", "city", "state", "zip_code", "county",
    "construction_code", "fire_alarm", "sprinkler_system",
]

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://192.168.1.111:8000","http://localhost:8000","https://your-ngrok-sub.ngrok-free.dev"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

Base.metadata.create_all(bind=engine)

#These are the Global Functions

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _is_blank(v):
    return v is None or (isinstance(v, str) and v.strip() == "")

def get_airtable_url(table_name: str) -> str:
    return f'{AIRTABLE_API_URL}{table_name}'

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

def _parse_since(since: str | None) -> datetime:
    if not since:
        return datetime(1970,1,1, tzinfo=timezone.utc)
    try:
        if since.endswith("Z"):
            since = since[:-1] + "+00:00"
        return datetime.fromisoformat(since)
    except Exception:
        raise HTTPException(400, "Invalid 'since' (use iso 8601, e.g. 2025-09-27T23:10:00Z)")

@app.get("/health")
def health():
    return {
        "env_loaded": True, 
        "has_db": bool(os.environ.get("DATABASE_URL")),
        "has_airtable_key": bool(os.environ.get("AIRTABLE_API_KEY"))
    }

# These Routers are the Intial Buildings Airtable Routers (Only used for major overides)

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

# Production Level Airtable Routers

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

    sql = "UPDATE dbo.Building SET " + ", ".join(set_cols) + " WHERE building_id = ? AND is_deleted = 0"
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

@app.post("/airtable/buildings/restore")
def restore_building(payload: dict = Body(...)):
    bld_id = payload.get("building_id")
    if not bld_id:
        raise HTTPException(400, "building_id is required")
    sql = """
    UPDATE dbo.Building
        SET is_deleted = 0,
            deleted_at = NULL
     Where building_id = ? AND is_deleted = 1
    """
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (int(bld_id),))
        rows = cur.rowcount
        conn.commit()
        return {"status":"ok","restored":rows,"building_id":int(bld_id)}
    except pyodbc.Error as e:
        raise HTTPException(500, f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass


@app.post("/airtable/buildings/delete")
def soft_delete_building(payload: dict = Body(...)):
    bld_id = payload.get("building_id")
    if not bld_id:
        raise HTTPException(400, "building_id is required")

    sql = """
        UPDATE dbo.Building
        SET is_deleted = 1,
            deleted_at = SYSUTCDATETIME()
        WHERE building_id = ? AND is_deleted = 0

    """
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (int(bld_id),))
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "deleted": rows, "buildin_id": int(bld_id)}
    except pyodbc.Error as e:
        raise HTTPException(500, f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.get("/airtable/buildings/changes")
def buildings_changes(since: str = Query(None, description="ISO-8601, e.g. 2025-09-27T23:10:00Z")):
    since_dt = _parse_since(since)
    now = datetime.now(timezone.utc)

    sql = """
    SELECT
      b.building_id,
      b.mortgagee_id,
      b.[Address Normalized],
      b.[Bld#],
      b.[Owner Occupied],
      b.[Street Address],
      b.[City],
      b.[State],
      b.[Zip],
      b.[County],
      b.[Units],
      b.[construction_code],
      b.[Year Built],
      b.[Stories],
      b.[Square Feet],
      b.[Desired Building Coverage],
      b.[Fire Alarm],
      b.[Sprinkler System],
      b.[roof_year_updated],
      b.[plumbing_year_updated],
      b.[electrical_year_updated],
      b.[hvac_year_updated],
      b.[entity_id],
      e.legal_name AS [Entity Legal Name],   -- <-- added
      b.is_deleted,
      b.updated_at
    FROM dbo.[Building] b
    LEFT JOIN dbo.[Entity] e
      ON e.[Entity_Id] = b.[entity_id]
    WHERE b.[updated_at] > ? AND b.[updated_at] <= ?
    ORDER BY b.[updated_at], b.[building_id]
    """

    rows = []
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (since_dt, now))
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            rows.append(dict(zip(cols, r)))
    finally:
        try: conn.close()
        except: pass

    upserts = []
    deletes = []
    for r in rows:
        payload = {
            "building_id": r["building_id"],
            "mortgagee_id": r["mortgagee_id"],
            "Address Normalized": r["Address Normalized"],
            "Bld#": r["Bld#"],
            "Owner Occupied": r["Owner Occupied"],
            "Street Address": r["Street Address"],
            "City": r["City"],
            "State": r["State"],
            "Zip": r["Zip"],
            "County": r["County"],
            "Units": r["Units"],
            "construction_code": r["construction_code"],
            "Year Built": r["Year Built"],
            "Stories": r["Stories"],
            "Square Feet": r["Square Feet"],
            "Desired Building Coverage": r["Desired Building Coverage"],
            "Fire Alarm": r["Fire Alarm"],
            "Sprinkler System": r["Sprinkler System"],
            "roof_year_updated": r["roof_year_updated"],
            "plumbing_year_updated": r["plumbing_year_updated"],
            "electrical_year_updated": r["electrical_year_updated"],
            "hvac_year_updated": r["hvac_year_updated"],
            "entity_id": r["entity_id"],
            "Entity Legal Name": r["Entity Legal Name"],
            "updated_at": r["updated_at"].isoformat()
        }
        if r["is_deleted"]:
            deletes.append({"building_id": r["building_id"], "updated_at": payload["updated_at"]})
        else:
            upserts.append(payload)

    return {
        "now": now.isoformat(),
        "upserts": upserts,
        "deletes": deletes
}


@app.post("/airtable/entity/ingest")
def ingest_entity_from_airtable(payload: dict = Body(...)):
    """
    Creates a new row in dbo.Entity from Airtable.
    Accepts:
        - { "fields": {...} }
        - { "records":[{"fields":{...}}] }
        - raw fields dict
    Returns: {status, entity_id}
    """

    fields = payload.get("fields")
    if fields is None and isinstance(payload.get("records"), list) and payload["records"]:
        fields = payload["records"][0].get("fields", {})
    if fields is None:
        fields = payload

    norm = {}
    for k, v in (fields or {}).items():
        kk = (k or "").strip()
        key_snake = kk.lower().replace(" ", "_")
        if kk == "Entity_id":            norm["entity_id"] = v;             continue
        if kk == "State Registration":   norm["state_registration"] = v;    continue
        if kk == "Entity Start Date":    norm["entity_start_date"] = v;     continue
        norm[key_snake] = v
    f = norm

    legal_name = (f.get("legal_name") or "").strip()
    if not legal_name:
        raise HTTPException(status_code=400, detail="legal_name is required")

    state_reg = f.get("state_registration")
    if state_reg is not None:
        state_reg = (str(state_reg).strip() or None)
        if state_reg:
            state_reg = state_reg.upper()
            # enforce 2-letter rule early (same as CK_Entity_StateCode)
            if len(state_reg) != 2 or not state_reg.isalpha():
                raise HTTPException(status_code=400, detail="State Registration must be 2 letters (e.g., 'WV')")
    
    start_date = f.get("entity_start_date")
    if start_date:
        try:
            # allow '2024-05-01' or '2024-05-01T00:00:00.000Z'
            s = str(start_date)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            start_date = dt.date()
        except Exception:
            raise HTTPException(status_code=400, detail="Entity Start Date must be ISO date (YYYY-MM-DD)")

    fein = f.get("fein")
    if fein is not None:
        fein = str(fein).strip() or None

    sos_url = f.get("sos_url")
    if sos_url is not None:
        sos_url = str(sos_url).strip() or None

    sql = """
    INSERT INTO dbo.[Entity] (
        [legal_name],
        [State Registration],
        [Entity Start Date],
        [FEIN],
        [sos_url]
    ) 
    OUTPUT INSERTED.[Entity_Id]
    VALUES (?, ?, ?, ?, ?);
    """
    params = (legal_name, state_reg, start_date, fein, sos_url)
    
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dbo.[Entity] (
                [legal_name], [State Registration], [Entity Start Date], [FEIN], [sos_url]
            )
            OUTPUT INSERTED.[Entity_Id]
            VALUES (?, ?, ?, ?, ?);
        """, (legal_name, state_reg, start_date, fein, sos_url))
        entity_id = int(cur.fetchone()[0])
        conn.commit()
        entity_id = int(entity_id)
        return {"status": "ok", "entity_id": entity_id}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.post("/airtable/entity/update")
def update_entity_from_airtable(payload: dict = Body(...)):
    """
    Column-selective UPDATE for dbo.Entity.
    Accepts:
      - { "fields": { ... } }
      - { "records": [ { "fields": { ... } } ] }
      - raw fields dict
    Requires: entity_id in the payload.
    """
    fields = payload.get("fields")
    if fields is None and isinstance(payload.get("records"), list) and payload["records"]:
        fields = payload["records"][0].get("fields", {})
    if fields is None:
        fields = payload

    norm = {}
    for k, v in (fields or {}).items():
        kk = (k or "").strip()
        if kk == "Entity_Id":               norm["entity_id"] = v;               continue
        if kk == "State Registration":      norm["state_registration"] = v;      continue
        if kk == "Entity Start Date":       norm["entity_start_date"] = v;       continue
        norm[kk.lower().replace(" ", "_")] = v
    fields = norm

    ent_id = fields.get("entity_id")
    if ent_id in (None, ""):
        raise HTTPException(status_code=400, detail="entity_id is required for updates")

    to_set = {}
    if "legal_name" in fields:
        ln = (fields["legal_name"] or "").strip()
        if not ln:
            raise HTTPException(status_code=400, detail="legal_name cannot be blank")
        to_set["legal_name"] = ln

    if "state_registration" in fields:
        sr = fields["state_registration"]
        sr = (str(sr).strip() or None)
        if sr:
            sr = sr.upper()
            if len(sr) != 2 or not sr.isalpha():
                raise HTTPException(status_code=400, detail="State Registration must be 2 letters (e.g., 'WV')")
        to_set["State Registration"] = sr  # pass None to clear, or 2-letter code

    if "entity_start_date" in fields:
        esd = fields["entity_start_date"]
        if esd in (None, ""):
            to_set["Entity Start Date"] = None
        else:
            s = str(esd).strip()
            try:
                # Accept ISO date, ISO datetime(with/without Z), or US mm/dd/yyyy
                if s.endswith("Z"):
                    s = s[:-1] + "+00:00"
                if len(s) >= 10 and s[4] == "-" and s[7] == "-":
                    # YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS...
                    from datetime import datetime, date
                    dt = datetime.fromisoformat(s[:10])  # take the date part
                    to_set["Entity Start Date"] = date(dt.year, dt.month, dt.day)
                elif "/" in s:
                    from datetime import datetime, date
                    dt = datetime.strptime(s, "%m/%d/%Y")
                    to_set["Entity Start Date"] = date(dt.year, dt.month, dt.day)
                else:
                    raise ValueError("bad format")
            except Exception:
                raise HTTPException(status_code=400, detail="Entity Start Date must be ISO YYYY-MM-DD or mm/dd/yyyy")

    if "fein" in fields:
        fein = fields["fein"]
        fein = str(fein).strip() if fein is not None else None
        to_set["FEIN"] = fein

    if "sos_url" in fields:
        sos = fields["sos_url"]
        sos = str(sos).strip() if sos is not None else None
        to_set["sos_url"] = sos

    # Nothing to update?
    if not to_set:
        return {"status": "ok", "updated": 0, "entity_id": int(ent_id)}

    # Build SET clause
    set_cols = [f"[{col}] = ?" for col in to_set.keys()]
    set_vals = list(to_set.values())
    set_vals.append(int(ent_id))

    sql = "UPDATE dbo.[Entity] SET " + ", ".join(set_cols) + " WHERE [Entity_Id] = ?"

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, set_vals)
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "updated": rows, "entity_id": int(ent_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

from fastapi import Body, HTTPException
import pyodbc

@app.post("/airtable/entity/delete")
def soft_delete_entity(payload: dict = Body(...)):
    ent_id = payload.get("entity_id")
    if not ent_id:
        raise HTTPException(status_code=400, detail="entity_id is required")
    sql = """
      UPDATE dbo.[Entity]
         SET is_deleted = 1,
             deleted_at = SYSUTCDATETIME()
       WHERE [Entity_Id] = ? AND is_deleted = 0;
    """
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (int(ent_id),))
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "deleted": rows, "entity_id": int(ent_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.post("/airtable/entity/restore")
def restore_entity(payload: dict = Body(...)):
    ent_id = payload.get("entity_id")
    if not ent_id:
        raise HTTPException(status_code=400, detail="entity_id is required")
    sql = """
      UPDATE dbo.[Entity]
         SET is_deleted = 0,
             deleted_at = NULL
       WHERE [Entity_Id] = ? AND is_deleted = 1;
    """
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (int(ent_id),))
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "restored": rows, "entity_id": int(ent_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.get("/airtable/entity/changes")
def entity_changes(since: str = Query(None, description="ISO-8601, e.g. 2025-09-27T23:10:00Z")):
    since_dt = _parse_since(since)
    now = datetime.now(timezone.utc)

    sql = """
    SELECT
      e.[Entity_Id],
      e.[legal_name],
      e.[State Registration],
      e.[Entity Start Date],
      e.[FEIN],
      e.[sos_url],
      e.[is_deleted],
      e.[updated_at]
    FROM dbo.[Entity] e
    WHERE e.[updated_at] > ? AND e.[updated_at] <= ?
    ORDER BY e.[updated_at], e.[Entity_Id]
    """
    rows = []
    try:
        conn = pyodbc.connect(CONN_STR)
        cur  = conn.cursor()
        cur.execute(sql, (since_dt, now))
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            rows.append(dict(zip(cols, r)))
    finally:
        try: conn.close()
        except: pass

    upserts, deletes = [], []
    for r in rows:
        payload = {
            "Entity_Id": r["Entity_Id"],
            "legal_name": r["legal_name"],
            "State Registration": r["State Registration"],
            "Entity Start Date": r["Entity Start Date"].isoformat() if r["Entity Start Date"] else None,
            "FEIN": r["FEIN"],
            "sos_url": r["sos_url"],
            "updated_at": r["updated_at"].isoformat()
        }
        if r["is_deleted"]:
            deletes.append({"Entity_Id": r["Entity_Id"], "updated_at": payload["updated_at"]})
        else:
            upserts.append(payload)

    return {"now": now.isoformat(), "upserts": upserts, "deletes": deletes}

@app.post("/airtable/clientcontact/ingest")
def ingest_clientcontact_from_airtable(payload: dict = Body(...)):
    """
    Create a new row in dbo.ClientContact.
    Accepts { "fields": {...} }, { "records":[{"fields":{...}}] }, or raw dict.
    Requires: first_name, last_name, is_primary (0/1).
    Returns: {status, client_contact_id}
    """

    # 1) Unwrap Airtable envelopes
    fields = None
    if isinstance(payload, dict):
        if isinstance(payload.get("fields"), dict):
            fields = payload["fields"]
        elif isinstance(payload.get("records"), list) and payload["records"]:
            fields = payload["records"][0].get("fields", {})
    if fields is None:
        fields = payload if isinstance(payload, dict) else {}

    # 2) Normalize keys (allow Airtable labels or snake_case)
    alias = {
        "First Name": "first_name",
        "Last Name": "last_name",
        "Mailing Address": "mailing_address",
        "Physical Address": "physical_address",
        "Phone": "phone",
        "Email": "email",
        "Is Primary": "is_primary",
        "Parent Contact Id": "parent_contact_id",
        "Parent Contact ID": "parent_contact_id",
    }
    f = {}
    for k, v in (fields or {}).items():
        key = alias.get(k, k)
        key = (key or "").strip().lower().replace(" ", "_")
        f[key] = v

    # 3) Validate requireds
    first_name = (f.get("first_name") or "").strip()
    last_name  = (f.get("last_name")  or "").strip()
    if not first_name:
        raise HTTPException(status_code=400, detail="first_name is required")
    if not last_name:
        raise HTTPException(status_code=400, detail="last_name is required")
    if f.get("is_primary") in (None, ""):
        raise HTTPException(status_code=400, detail="is_primary is required (0 or 1)")

    # Correct 0/1 coercion (handles 0/1, "0"/"1", true/false)
    def coerce_01(v):
        if v is None: return None
        if isinstance(v, bool): return 1 if v else 0
        s = str(v).strip().lower()
        if s in ("1", "true", "t", "yes", "y"): return 1
        if s in ("0", "false", "f", "no", "n"): return 0
        return int(v)  # last resort for numerics

    is_primary = coerce_01(f.get("is_primary"))
    if is_primary not in (0, 1):
        raise HTTPException(status_code=400, detail="is_primary must be 0 or 1")

    mailing_address  = f.get("mailing_address")
    physical_address = f.get("physical_address")
    phone            = f.get("phone")
    email            = f.get("email")
    email = email.lower().strip() if isinstance(email, str) and email.strip() else None
    parent_contact_id = f.get("parent_contact_id")
    if parent_contact_id in ("", None):
        parent_contact_id = None
    else:
        try:
            parent_contact_id = int(parent_contact_id)
        except Exception:
            raise HTTPException(status_code=400, detail="parent_contact_id must be an integer")

    # 4) Insert (trigger/NOCOUNT safe)
    try:
        conn = pyodbc.connect(CONN_STR)
        cur  = conn.cursor()

        # temp table to capture new id
        cur.execute("IF OBJECT_ID('tempdb..#ids') IS NOT NULL DROP TABLE #ids; CREATE TABLE #ids (id INT);")

        cur.execute("""
            INSERT INTO dbo.ClientContact
                (first_name, last_name, is_primary,
                 mailing_address, physical_address, phone, email, parent_contact_id,
                 is_deleted, deleted_at, updated_at)
            OUTPUT INSERTED.client_contact_id INTO #ids
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, SYSDATETIME());
        """, (
            first_name, last_name, is_primary,
            mailing_address, physical_address, phone, email, parent_contact_id
        ))

        cur.execute("SELECT id FROM #ids;")
        row = cur.fetchone()
        if not row or row[0] is None:
            raise HTTPException(status_code=500, detail="Insert succeeded but no ID was captured")
        new_id = int(row[0])

        conn.commit()
        return {"status": "ok", "client_contact_id": new_id}

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except Exception: pass

@app.post("/airtable/clientcontact/update")
def update_clientcontact_from_airtable(payload: dict = Body(...)):
    """
    Column-selective UPDATE for dbo.ClientContact.
    Accepts:
      - { "fields": { ... } }
      - { "records": [ { "fields": { ... } } ] }
      - raw fields dict
    Requires: client_contact_id in the payload.
    """
    # 1) Unwrap Airtable-style envelope
    fields = None
    if isinstance(payload, dict):
        fields = payload.get("fields")
        if fields is None and isinstance(payload.get("records"), list) and payload["records"]:
            fields = payload["records"][0].get("fields", {})
    if fields is None:
        fields = payload or {}

    # 2) Normalize keys (Airtable labels -> snake_case)
    alias = {
        "Client Contact Id": "client_contact_id",
        "Client Contact ID": "client_contact_id",
        "First Name": "first_name",
        "Last Name": "last_name",
        "Mailing Address": "mailing_address",
        "Physical Address": "physical_address",
        "Phone": "phone",
        "Email": "email",
        "Is Primary": "is_primary",
        "Parent Contact Id": "parent_contact_id",
        "Parent Contact ID": "parent_contact_id",
    }
    f = {}
    for k, v in (fields or {}).items():
        key = alias.get(k, k)
        key = (key or "").strip().lower().replace(" ", "_")
        f[key] = v

    # 3) Validate PK
    cc_id = f.get("client_contact_id")
    if cc_id in (None, ""):
        raise HTTPException(status_code=400, detail="client_contact_id is required for updates")

    # 4) Build SET list only for provided columns (allow clearing with empty string/None)
    to_set = {}

    def _clean_str(val):
        # Convert "" -> None; strip non-None strings
        if val is None:
            return None
        s = str(val).strip()
        return s if s else None

    if "first_name" in f:
        val = _clean_str(f["first_name"])
        if val is None:
            raise HTTPException(status_code=400, detail="first_name cannot be blank if provided")
        to_set["first_name"] = val

    if "last_name" in f:
        val = _clean_str(f["last_name"])
        if val is None:
            raise HTTPException(status_code=400, detail="last_name cannot be blank if provided")
        to_set["last_name"] = val

    if "mailing_address" in f:
        to_set["mailing_address"] = _clean_str(f["mailing_address"])

    if "physical_address" in f:
        to_set["physical_address"] = _clean_str(f["physical_address"])

    if "phone" in f:
        to_set["phone"] = _clean_str(f["phone"])

    if "email" in f:
        # normalize "" -> NULL; keep lower/trim if present
        email = _clean_str(f["email"])
        to_set["email"] = email.lower() if isinstance(email, str) else None

    if "is_primary" in f:
        # any truthy -> 1, falsy -> 0
        to_set["is_primary"] = int(bool(f["is_primary"]))

    if "parent_contact_id" in f:
        val = f["parent_contact_id"]
        if val in (None, ""):
            to_set["parent_contact_id"] = None
        else:
            try:
                to_set["parent_contact_id"] = int(val)
            except Exception:
                raise HTTPException(status_code=400, detail="parent_contact_id must be an integer")

    # Nothing to update?
    if not to_set:
        return {"status": "ok", "updated": 0, "client_contact_id": int(cc_id)}

    # 5) Build SQL
    set_cols = [f"[{col}] = ?" for col in to_set.keys()]
    set_vals = list(to_set.values())
    set_vals.append(int(cc_id))

    sql = (
        "UPDATE dbo.[ClientContact] "
        + "SET " + ", ".join(set_cols)
        + " WHERE [client_contact_id] = ? AND is_deleted = 0"
    )

    # 6) Execute
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, set_vals)
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "updated": rows, "client_contact_id": int(cc_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.post("/airtable/clientcontact/delete")
def delete_clientcontact(payload: dict = Body(...)):
    """
    Soft-delete: set is_deleted=1 and timestamp deleted_at.
    Accepts { client_contact_id } or { fields: { Client Contact ID: ... } } etc.
    """
    # Unwrap common Airtable shapes
    cc_id = None
    if isinstance(payload, dict):
        if "client_contact_id" in payload:
            cc_id = payload["client_contact_id"]
        elif "fields" in payload and isinstance(payload["fields"], dict):
            cc_id = payload["fields"].get("client_contact_id") or payload["fields"].get("Client Contact ID")
        elif "records" in payload and isinstance(payload["records"], list) and payload["records"]:
            cc_id = payload["records"][0].get("fields", {}).get("client_contact_id") or payload["records"][0].get("fields", {}).get("Client Contact ID")

    if cc_id in (None, ""):
        raise HTTPException(status_code=400, detail="client_contact_id is required")

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.ClientContact
            SET is_deleted = 1,
                deleted_at = SYSDATETIME()
            WHERE client_contact_id = ?
            """,
            (int(cc_id),),
        )
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "deleted": rows, "client_contact_id": int(cc_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


@app.post("/airtable/clientcontact/restore")
def restore_clientcontact(payload: dict = Body(...)):
    """
    Restore: set is_deleted=0 and clear deleted_at.
    Accepts { client_contact_id } or Airtable-shaped envelopes.
    """
    # Unwrap common Airtable shapes
    cc_id = None
    if isinstance(payload, dict):
        if "client_contact_id" in payload:
            cc_id = payload["client_contact_id"]
        elif "fields" in payload and isinstance(payload["fields"], dict):
            cc_id = payload["fields"].get("client_contact_id") or payload["fields"].get("Client Contact ID")
        elif "records" in payload and isinstance(payload["records"], list) and payload["records"]:
            cc_id = payload["records"][0].get("fields", {}).get("client_contact_id") or payload["records"][0].get("fields", {}).get("Client Contact ID")

    if cc_id in (None, ""):
        raise HTTPException(status_code=400, detail="client_contact_id is required")

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE dbo.ClientContact
            SET is_deleted = 0,
                deleted_at = NULL
            WHERE client_contact_id = ?
            """,
            (int(cc_id),),
        )
        rows = cur.rowcount
        conn.commit()
        return {"status": "ok", "restored": rows, "client_contact_id": int(cc_id)}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.get("/airtable/clientcontact/changes")
def clientcontact_changes(since: str = Query(None, description="ISO-8601, e.g. 2025-09-27T23:10:00Z")):
    since_dt = _parse_since(since)
    now = datetime.now(timezone.utc)

    sql = """
    SELECT
      c.client_contact_id,
      c.first_name,
      c.last_name,
      c.mailing_address,
      c.physical_address,
      c.phone,
      c.email,
      c.is_primary,
      c.parent_contact_id,
      c.is_deleted,
      c.updated_at
    FROM dbo.ClientContact c
    WHERE c.updated_at > ? AND c.updated_at <= ?
    ORDER BY c.updated_at, c.client_contact_id
    """

    rows = []
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (since_dt, now))
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            rows.append(dict(zip(cols, r)))
    finally:
        try:
            conn.close()
        except:
            pass

    upserts, deletes = [], []
    for r in rows:
        payload = {
            "client_contact_id": r["client_contact_id"],
            "first_name": r["first_name"],
            "last_name": r["last_name"],
            "mailing_address": r["mailing_address"],
            "physical_address": r["physical_address"],
            "Phone": r["phone"],
            "Email": r["email"],
            "is_primary": r["is_primary"],
            "parent_contact_id": r["parent_contact_id"],
            "updated_at": r["updated_at"].isoformat()
        }
        if r["is_deleted"]:
            deletes.append({
                "client_contact_id": r["client_contact_id"],
                "updated_at": payload["updated_at"]
            })
        else:
            upserts.append(payload)

    return {"now": now.isoformat(), "upserts": upserts, "deletes": deletes}

@app.get("/airtable/building_contact/changes")
def building_contact_changes(since: str = Query(None, description="ISO-8601, e.g. 2025-09-27T23:10:00Z")):
    since_dt = _parse_since(since)
    now = datetime.now(timezone.utc)

    sql = """
    SELECT
      bc.building_contact_id,
      bc.building_id,
      bc.client_contact_id,
      bc.role,
      bc.is_primary,
      bc.is_active,
      bc.updated_at
    FROM dbo.Building_Contact bc
    WHERE bc.updated_at > ? AND bc.updated_at <= ?
    ORDER BY bc.updated_at, bc.building_contact_id
    """

    rows = []
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute(sql, (since_dt, now))
        cols = [c[0] for c in cur.description]
        for r in cur.fetchall():
            rows.append(dict(zip(cols, r)))
    finally:
        try: conn.close()
        except: pass

    upserts, deletes = [], []
    for r in rows:
        payload = {
            "building_contact_id": r["building_contact_id"],
            "building_id":        r["building_id"],
            "client_contact_id":  r["client_contact_id"],
            "role":               r["role"],
            "is_primary":         int(r["is_primary"]) if r["is_primary"] is not None else None,
            "is_active":          int(r["is_active"])  if r["is_active"]  is not None else None,
            "updated_at":         r["updated_at"].isoformat()
        }
        if r["is_active"] == 0:
            deletes.append({"building_contact_id": r["building_contact_id"], "updated_at": payload["updated_at"]})
        else:
            upserts.append(payload)

    return {"now": now.isoformat(), "upserts": upserts, "deletes": deletes}


###************************************###
###         Custom View Routers        ###
###************************************###

@app.get("/airtable/clientcontact_policies_via_building/changes")
def clientcontact_policies_via_building_changes(
    client_contact_id: Optional[int] = None,
    policy_number: Optional[str] = None,
    carrier: Optional[str] = None,
    building_id: Optional[int] = None,
):
    where, params = [], []
    if client_contact_id is not None:
        where.append("client_contact_id = ?"); params.append(client_contact_id)
    if policy_number:
        where.append("[policy_number] = ?"); params.append(policy_number)
    if carrier:
        where.append("[carrier] = ?"); params.append(carrier)
    if building_id is not None:
        where.append("building_id = ?"); params.append(building_id)

    sql = """
      SELECT
        client_contact_id, contact_name, item_type, building_id, policy_id,
        policy_number, carrier, address_normalized,
        City, State, zip_code, [Entity Legal Name], desired_building_coverage
      FROM dbo.v_ClientContact_PoliciesViaBuilding
    """
    if where:
        sql += " WHERE " + " AND ".join(where)
    
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        cols = [c[0] for c in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()

    upserts = []
    for r in rows:
        pk = f"{r['client_contact_id']}|{r['building_id']}|{r['policy_id']}"
        upserts.append({
            "_pk": pk,
            "client_contact_id": r["client_contact_id"],
            "contact_name": r["contact_name"],
            "item_type": r["item_type"],
            "building_id": r["building_id"],
            "policy_id": r["policy_id"],
            "policy_number": r["policy_number"],
            "carrier": r["carrier"],
            "address_normalized": r["address_normalized"],
            "City": r["City"],
            "State": r["State"],
            "zip_code": r["zip_code"],
            "Entity Legal Name": r["Entity Legal Name"],
            "desired_building_coverage": r["desired_building_coverage"],
        })
    return {"upserts": upserts, "deletes": []}

###************************************###
###Old Client Swagger UI middle routers###
###************************************###

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

###************************************###
###New Client Swagger UI middle routers###
###************************************###

@app.post("/clientcontacts/")
def create_client_contact(payload: dict = Body(
    ...,
    description="Create a ClientContact (snake_case, is_primary is 0/1)",
    example={
        "first_name": "Jane",
        "last_name": "Doe",
        "is_primary": 1,
        "mailing_address": "123 Main St, Charleston, WV 25301",
        "physical_address": "123 Main St, Charleston, WV 25301",
        "phone": "304-555-1234",
        "email": "jane.doe@example.com",
        "parent_contact_id": None
    }
)):
    for req in ("first_name", "last_name", "is_primary"):
        if req not in payload or payload[req] in (None, ""):
            raise HTTPException(status_code=400, detail=f"Missing required: {req}")

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()

        cur.execute("IF OBJECT_ID('tempdb..#ids') IS NOT NULL DROP TABLE #ids; CREATE TABLE #ids (id INT);")

        cur.execute("""
            INSERT INTO dbo.ClientContact
                (first_name, last_name, is_primary,
                 mailing_address, physical_address, phone, email, parent_contact_id,
                 is_deleted, deleted_at, updated_at)
            OUTPUT INSERTED.client_contact_id INTO #ids
            VALUES
                (?, ?, ?,
                 ?, ?, ?, ?, ?,
                 0, NULL, SYSDATETIME());
        """, (
            str(payload.get("first_name")).strip(),
            str(payload.get("last_name")).strip(),
            int(payload.get("is_primary")),
            payload.get("mailing_address"),
            payload.get("physical_address"),
            payload.get("phone"),
            (payload.get("email") or None).lower() if payload.get("email") else None,
            payload.get("parent_contact_id"),
        ))

        cur.execute("SELECT id FROM #ids;")
        row = cur.fetchone()
        if not row or row[0] is None:
            raise HTTPException(status_code=500, detail="Insert succeeded but no ID was captured")
        new_id = int(row[0])

        cur.execute("""
            SELECT client_contact_id, first_name, last_name, is_primary,
                   mailing_address, physical_address, phone, email, parent_contact_id,
                   is_deleted,
                   CONVERT(VARCHAR(19), deleted_at, 126) AS deleted_at,
                   CONVERT(VARCHAR(19), updated_at, 126) AS updated_at
            FROM dbo.ClientContact
            WHERE client_contact_id = ?;
        """, (new_id,))
        out = cur.fetchone()
        if not out:
            raise HTTPException(status_code=500, detail="Insert OK but row fetch failed")

        conn.commit()
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, out))

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.get("/clientcontacts/{client_contact_id}")
def read_client_contact(client_contact_id: int):
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()
        cur.execute("""
            SELECT client_contact_id, first_name, last_name, is_primary,
                   mailing_address, physical_address, phone, email, parent_contact_id,
                   is_deleted,
                   CONVERT(VARCHAR(19), deleted_at, 126) AS deleted_at,
                   CONVERT(VARCHAR(19), updated_at, 126) AS updated_at
            FROM dbo.ClientContact
            WHERE client_contact_id = ?;
        """, (client_contact_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="ClientContact not found")
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.put("/clientcontacts/{client_contact_id}")
def update_client_contact(
    client_contact_id: int,
    payload: dict = Body(
        ...,
        description="Partial update; send only fields you want to change (snake_case, is_primary is 0/1)",
        example={
            "first_name": "Janet",
            "last_name": "Doe-Smith",
            "is_primary": 0,
            "mailing_address": "456 Oak Ave, Huntington, WV 25701",
            "email": "janet.doe@example.com",
            "parent_contact_id": 42
        }
    )
):
    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()

        cur.execute("SELECT 1 FROM dbo.ClientContact WHERE client_contact_id = ?", (client_contact_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="ClientContact not found")

        updates, params = [], []

        def add(col, val):
            updates.append(f"{col} = ?")
            params.append(val)

        if "first_name" in payload:        add("first_name", str(payload["first_name"]).strip() if payload["first_name"] is not None else None)
        if "last_name" in payload:         add("last_name",  str(payload["last_name"]).strip()  if payload["last_name"]  is not None else None)
        if "is_primary" in payload:        add("is_primary", int(payload["is_primary"]) if payload["is_primary"] is not None else None)

        if "mailing_address" in payload:   add("mailing_address",  payload["mailing_address"])
        if "physical_address" in payload:  add("physical_address", payload["physical_address"])
        if "phone" in payload:             add("phone", payload["phone"])
        if "email" in payload:
            email = payload["email"]
            add("email", email.lower() if isinstance(email, str) else None)
        if "parent_contact_id" in payload: add("parent_contact_id", payload["parent_contact_id"])

        if not updates:
            cur.execute("""
                SELECT client_contact_id, first_name, last_name, is_primary,
                       mailing_address, physical_address, phone, email, parent_contact_id,
                       is_deleted,
                       CONVERT(VARCHAR(19), deleted_at, 126) AS deleted_at,
                       CONVERT(VARCHAR(19), updated_at, 126) AS updated_at
                FROM dbo.ClientContact
                WHERE client_contact_id = ?;
            """, (client_contact_id,))
            row = cur.fetchone()
            cols = [c[0] for c in cur.description]
            return dict(zip(cols, row))

        updates.append("updated_at = SYSDATETIME()")
        sql = f"UPDATE dbo.ClientContact SET {', '.join(updates)} WHERE client_contact_id = ?"

        cur.execute(sql, params + [client_contact_id])
        conn.commit()

        cur.execute("""
            SELECT client_contact_id, first_name, last_name, is_primary,
                   mailing_address, physical_address, phone, email, parent_contact_id,
                   is_deleted,
                   CONVERT(VARCHAR(19), deleted_at, 126) AS deleted_at,
                   CONVERT(VARCHAR(19), updated_at, 126) AS updated_at
            FROM dbo.ClientContact
            WHERE client_contact_id = ?;
        """, (client_contact_id,))
        row = cur.fetchone()
        cols = [c[0] for c in cur.description]
        return dict(zip(cols, row))

    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.post("/clientcontacts/soft-delete")
def clientcontacts_soft_delete(payload: IdList = Body(
    ...,
    example={"ids": [12, 13, 27]},
    description="List of client_contact_id values to SOFT delete"
)):
    id_list = sorted(set(payload.ids))
    if not id_list:
        raise HTTPException(status_code=400, detail="Provide one or more client_contact_id values")

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()

        ph = ",".join("?" for _ in id_list)
        cur.execute(f"SELECT client_contact_id FROM dbo.ClientContact WHERE client_contact_id IN ({ph})", id_list)
        existing = {int(r[0]) for r in cur.fetchall()}
        not_found = [i for i in id_list if i not in existing]
        if not existing:
            return {"status": "ok", "soft_deleted": 0, "not_found": not_found}

        cur.execute(
            f"""
            UPDATE dbo.ClientContact
            SET is_deleted = 1,
                deleted_at  = SYSDATETIME(),
                updated_at  = SYSDATETIME()
            WHERE client_contact_id IN ({ph})
            """,
            list(existing)
        )
        affected = cur.rowcount
        conn.commit()
        return {"status": "ok", "soft_deleted": affected, "ids": sorted(existing), "not_found": not_found}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

@app.post("/clientcontacts/hard-delete")
def clientcontacts_hard_delete(payload: IdList = Body(
    ...,
    example={"ids": [12, 13, 27]},
    description="List of client_contact_id values to HARD delete (permanent)"
)):
    id_list = sorted(set(payload.ids))
    if not id_list:
        raise HTTPException(status_code=400, detail="Provide one or more client_contact_id values")

    try:
        conn = pyodbc.connect(CONN_STR)
        cur = conn.cursor()

        ph = ",".join("?" for _ in id_list)
        cur.execute(f"SELECT client_contact_id FROM dbo.ClientContact WHERE client_contact_id IN ({ph})", id_list)
        existing = {int(r[0]) for r in cur.fetchall()}
        not_found = [i for i in id_list if i not in existing]
        if not existing:
            return {"status": "ok", "hard_deleted": 0, "not_found": not_found}

        cur.execute(f"DELETE FROM dbo.ClientContact WHERE client_contact_id IN ({ph})", list(existing))
        affected = cur.rowcount
        conn.commit()
        return {"status": "ok", "hard_deleted": affected, "ids": sorted(existing), "not_found": not_found}
    except pyodbc.Error as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
    finally:
        try: conn.close()
        except: pass

