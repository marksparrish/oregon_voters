import sqlalchemy

TABLENAME = "PRECINCTS"

DATA_CONTRACT = {
    'district_county': ['COUNTY'],
    'district_type': ['DISTRICT_TYPE'],
    'district_name': ['DISTRICT_NAME'],
    'precinct_number': ['PRECINCT_CODE','PRECINCT'],
    'precinct_name': ['PRECINCT_NAME'],
    'precinct_split': ['SPLIT_CODE','SPLIT'],
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
    'file_date',
    'state',
    'district_county',
    'district_type',
    'district_name',
    'district_link',
    'precinct_name',
    'precinct_number',
    'precinct_split',
    'precinct_link',
]
