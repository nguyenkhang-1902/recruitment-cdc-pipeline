from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os

# Using the service name 'mysql' as defined in your docker-compose.yml
MYSQL_URL = os.getenv("DATABASE_URL", "mysql+pymysql://root:root@mysql:3306/recruitment_dw")

# Initialize SQLAlchemy engine for the Data Warehouse
engine = create_engine(MYSQL_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Dependency to provide a database session for each request
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()