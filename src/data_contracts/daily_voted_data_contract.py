import sqlalchemy

TABLENAME = "DAILY_VOTED"

DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
    'ballot_id': ['BALLOT_ID'],
    'ballot_style': ['BALLOT_STYLE'],
    'voted_on_date': ['RECEIVE_DATE'],
}

dtype_mapping = {
    'election_date': sqlalchemy.types.Date(),
    'state': sqlalchemy.types.String(length=50),
    'state_voter_id': sqlalchemy.types.String(length=100),
    'ballot_id': sqlalchemy.types.String(length=100),
    'ballot_style': sqlalchemy.types.String(length=200),
    'voted_on_date': sqlalchemy.types.Date(),
}

final_columns = [
    'state',
    'election_date',
    'state_voter_id',
    'ballot_id',
    'ballot_style',
    'voted_on_date',
]
