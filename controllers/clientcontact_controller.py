from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from models.clientcontact_model import ClientContact
from schemas.clientcontact_schema import ClientContactCreate, ClientContactUpdate, ClientContactInDB
from database import get_db

Router = APIRouter()

@Router.post("/clientcontacts/", response_model=ClientContactInDB)
def create_client_contact(client_contact: ClientContactCreate, db: Session = Depends(get_db)):
    db_client_contact = ClientContact(**client_contact.dict())
    db.add(db_client_contact)
    db.commit()
    db.refresh(db_client_contact)
    return db_client_contact

@Router.get("/clientcontacts/{client_contact_id}", response_model=ClientContactInDB)
def read_client_contact (client_contact_id: int, db: Session = Depends(get_db)):
    db_client_contact = db.query(ClientContact).filter(ClientContact.ClientContact_id == client_contact_id).first()
    if db_client_contact is None:
        raise HTTPException(status_code=404, detail="ClientContact not found")
    return db_client_contact

@Router.put("/clientcontacts/{client_contact_id}", response_model=ClientContactInDB)
def update_client_contact(client_contact_id: int, client_contact: ClientContactUpdate, db: Session = Depends(get_db)):
    db_client_contact = db.query(ClientContact).filter(ClientContact.ClientContact_id == client_contact_id).first()
    if db_client_contact is None:
        raise HTTPException(status_code=404, detail="ClientContact not found")
    for key, value in client_contact.dict(exclude_unset=True).items():
        setattr(db_client_contact, key, value)
    db.commit()
    return db_client_contact

@Router.delete("/clientcontacts/{client_contact_id}", response_model=ClientContactInDB)
def delete_client_contact(client_contact_id: int, db: Session = Depends(get_db)):
    db_client_contact = db.query(ClientContact).filter(ClientContact.ClientContact_id == client_contact_id).first()
    if db_client_contact is None:
        raise HTTPException(status_code=404, detail="ClientContact not found")
    db.delete(db_client_contact)
    db.commit()
    return db_client_contact

