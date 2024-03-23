import sqlalchemy

TABLENAME ="votehistory"

DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
}

dtype_mapping = {
    'state': sqlalchemy.types.String(length=50),
    'state_voter_id': sqlalchemy.types.String(length=50),
    'election_date': sqlalchemy.types.Date(),
    'voted': sqlalchemy.types.String(length=3),
    'votes_early_days': sqlalchemy.types.Integer(),
    'voted_on_date': sqlalchemy.types.Date(),
    }

final_columns = [
    'state',
    'state_voter_id',
    'election_date',
    'voted',
    'votes_early_days',
    'voted_on_date'
]
