import pytest
from fastapi.testclient import TestClient
from main import app  # Ensure you import your FastAPI app here

client = TestClient(app)

def test_create_building():
    response = client.post("/buildings/", json={
        "mortgagee_id": 1,
        "address_normalized": "123 Example St, Exampleland, WV 66666",
        "bld_number": 1,
        "owner_occupied": True,
        "street_address": "123 Example St",
        "city": "Exampleland",
        "state": "WV",
        "zip_code": "66666",
        "county": "Example County",
        "units": 10,
        "construction_code": 1,
        "year_built": 2000,
        "stories": 2,
        "square_feet": 2500,
        "desired_building_coverage": 1000000,
        "fire_alarm": True,
        "sprinkler_system": True,
        "roof_year_updated": 2020,
        "plumbing_year_updated": 2019,
        "electrical_year_updated": 2018,
        "hvac_year_updated": 2021,
        "entity_id": 1,
                
    })
    assert response.status_code == 200

# Add other test cases for read, update, delete
