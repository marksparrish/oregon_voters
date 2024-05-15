import sqlalchemy
from sqlalchemy import Column, Integer, String, Date, Index, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.config import file_date
from utils.database import Database

# Define the table name
TABLENAME = "DAILY_VOTED"

# Data mapping
DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
    'voted_on_date': ['RECEIVE_DATE'],
    'county': ['county'],
}

# Define data types for each column
dtype_mapping = {
    'state': sqlalchemy.types.String(length=50),
    'county': sqlalchemy.types.String(length=50),
    'state_voter_id': sqlalchemy.types.String(length=255),
    'election_date': sqlalchemy.types.Date(),
    'voted_on_date': sqlalchemy.types.Date(),
}

# Final columns list (seems unused here, may be used elsewhere)
final_columns = [
    'state',
    'county',
    'state_voter_id',
    'election_date',
    'voted_on_date',
]

# Using declarative_base to create the base class
Base = declarative_base()

# Definition of the DailyVoted class
class DailyVoted(Base):
    __tablename__ = f"daily_voted_{file_date.strftime('%Y_%m_%d')}"
    id = Column(sqlalchemy.Integer, primary_key=True)
    state = Column(sqlalchemy.types.String(length=50))
    county = Column(sqlalchemy.types.String(length=50))
    state_voter_id = Column(sqlalchemy.types.String(length=255))
    election_date = Column(sqlalchemy.types.Date())
    voted_on_date = Column(sqlalchemy.types.Date())
    # Add a unique index for state, election_date, state_voter_id, ballot_id
    __table_args__ = (
        Index('idx_state_voter_id', 'state_voter_id'),  # Index for state_voter_id
    )
