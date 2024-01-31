import sqlalchemy

TABLENAME ="votehistory"

DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
}

dtype_mapping = {
    'file_date': sqlalchemy.types.Date(),
    'state': sqlalchemy.types.String(length=50),
    'district_county': sqlalchemy.types.String(length=100),
    'district_type': sqlalchemy.types.String(length=100),
    'district_name': sqlalchemy.types.String(length=100),
    'district_link': sqlalchemy.types.String(length=200),
    'precinct_name': sqlalchemy.types.String(length=100),
    'precinct_number': sqlalchemy.types.String(length=100),
    'precinct_split': sqlalchemy.types.String(length=100),
    'precinct_link': sqlalchemy.types.String(length=200),
}

final_columns = [
    'state',
    'state_voter_id',
    'election_date',
    'voted',
    'votes_early_days',
    'voted_on_date'
]
