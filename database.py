from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, session
from sqlalchemy.ext.declarative import declarative_base

# Database URL
DATABASE_URL = 'mssql+pyodbc://wbis_api:a7#J!q9P%bZt$r2d@JOHN\\SQLEXPRESS01/wbis_core?driver=ODBC+Driver+17+for+SQL+Server'

# Create the database engine
engine = create_engine(DATABASE_URL)

# Create a configured "Session" class
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a base class for your models
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
