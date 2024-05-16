import sqlalchemy
from sqlalchemy import Column, Integer, String, Date, Index, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.config import file_date
from utils.database import Database
import pandas as pd

TABLENAME ="vote_history"

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

TABLENAME = "vote_history"

class ElectionVoting(Base):
    __tablename__ = 'table_name'
    id = Column(Integer, primary_key=True)
    state = Column(String(length=50))
    state_voter_id = Column(String(length=255))
    election_date = Column(Date)
    voted = Column(String(length=3))

    __table_args__ = (
        Index('idx_state_voter_id', 'state_voter_id'),
        Index('idx_election_date', 'election_date'),
    )

    @classmethod
    def set_table_name(cls, election_date):
        """
        Sets the table name dynamically based on the election_date.

        Parameters:
        election_date (datetime.date or str): The election date to use for the table name.
        """
        cls.__tablename__ = f"{TABLENAME.lower()}_{election_date}"
        cls.__table__.name = cls.__tablename__
        Base.metadata.tables[cls.__tablename__] = cls.__table__

def create_dynamic_class(election_date):
    """
    Creates a new dynamic class based on the election date.

    Parameters:
    election_date (datetime.date or str): The election date to use for the table name.

    Returns:
    type: A new SQLAlchemy ORM class with the appropriate table name.
    """
    table_name = f"{TABLENAME.lower()}_{election_date}"

    # Dynamically create a new class with the appropriate table name
    DynamicElectionVoting = type(
        f'ElectionVoting_{election_date}',
        (Base,),
        {
            '__tablename__': table_name,
            'id': Column(Integer, primary_key=True),
            'state': Column(String(length=50)),
            'state_voter_id': Column(String(length=255)),
            'election_date': Column(Date),
            'voted': Column(String(length=3)),
            '__table_args__': (
                Index('idx_state_voter_id', 'state_voter_id'),
                Index('idx_election_date', 'election_date'),)
        }
    )
    return DynamicElectionVoting
