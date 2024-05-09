import sqlalchemy
from sqlalchemy import Column, Integer, String, Date, Index, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.config import file_date

# Define the table name
TABLENAME = "DAILY_VOTED"

# Data mapping
DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
    'ballot_id': ['BALLOT_ID'],
    'ballot_style': ['BALLOT_STYLE'],
    'voted_on_date': ['RECEIVE_DATE'],
}

# Define data types for each column
dtype_mapping = {
    'election_date': sqlalchemy.types.Date(),
    'state': sqlalchemy.types.String(length=50),
    'state_voter_id': sqlalchemy.types.String(length=255),
    'ballot_id': sqlalchemy.types.String(length=100),
    'ballot_style': sqlalchemy.types.String(length=200),
    'voted_on_date': sqlalchemy.types.Date(),
}

# Final columns list (seems unused here, may be used elsewhere)
final_columns = [
    'state',
    'election_date',
    'state_voter_id',
    'ballot_id',
    'ballot_style',
    'voted_on_date',
]

# Using declarative_base to create the base class
Base = declarative_base()

# Definition of the DailyVoted class
class DailyVoted(Base):
    __tablename__ = f"daily_voted_{file_date.strftime('%Y_%m_%d')}"
    id = Column(sqlalchemy.Integer, primary_key=True)
    state = Column(sqlalchemy.types.String(length=50))
    election_date = Column(sqlalchemy.types.Date())
    state_voter_id = Column(sqlalchemy.types.String(length=255))
    ballot_id = Column(sqlalchemy.types.String(length=100))
    ballot_style = Column(sqlalchemy.types.String(length=200))
    voted_on_date = Column(sqlalchemy.types.Date())
    # Add a unique index for state, election_date, state_voter_id, ballot_id
    __table_args__ = (
        UniqueConstraint('state', 'election_date', 'state_voter_id', 'ballot_id', 'ballot_style', 'voted_on_date', name='idx_unique'),
        Index('idx_state_voter_id', 'state_voter_id'),  # Index for state_voter_id
    )

def create_daily_voted_table(engine):
    """
    Create the daily_voted table in the database.

    Parameters:
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine
    """
    Base.metadata.create_all(engine, checkfirst=True)
