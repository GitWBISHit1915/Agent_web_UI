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
    pass  # Additional validation can be added here if necessary

class BuildingUpdate(BaseModel):
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

class BuildingInDB(BuildingBase):
    building_id: int

    class Config:
        orm_mode = True  # Allows reading data as dict.
