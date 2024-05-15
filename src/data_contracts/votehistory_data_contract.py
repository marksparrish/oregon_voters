import sqlalchemy
from sqlalchemy import Column, Integer, String, Date, Index, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.config import file_date
from utils.database import Database

TABLENAME ="votehistory"

DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
}

dtype_mapping = {
    'state': sqlalchemy.types.String(length=50),
    'state_voter_id': sqlalchemy.types.String(length=50),
    'election_date': sqlalchemy.types.Date(),
    'voted': sqlalchemy.types.String(length=3),
}

final_columns = [
    'state',
    'state_voter_id',
    'election_date',
    'voted'
]

Base = declarative_base()

# Definition of the DailyVoted class
class ElectionVoting(Base):
    __tablename__ = f"election_voted_{file_date.strftime('%Y_%m_%d')}"
    id = Column(sqlalchemy.Integer, primary_key=True)
    state = Column(sqlalchemy.types.String(length=50))
    state_voter_id = Column(sqlalchemy.types.String(length=255))
    election_date = Column(sqlalchemy.types.Date())
    voted = Column(sqlalchemy.types.String(length=3))
    # Add a unique index for state, election_date, state_voter_id, ballot_id
    __table_args__ = (
        Index('idx_state_voter_id', 'state_voter_id'),  # Index for state_voter_id
    )
