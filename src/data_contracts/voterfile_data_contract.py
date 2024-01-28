"""
This file contains the data contract for the voterfile module.
The data contract is a dictionary that defines the columns that are
expected to be in the voter file and the columns that will be used
to populate the columns in the database.
The data contract also defines the data types for the columns in the
database.
"""
import sqlalchemy

TABLENAME = "voters"
# codes for active voters all others will be ignored
ACTIVE_VOTERS_CODES = ["A"]
# data contract to ensure needed votetracker fields are present
# form is 'votetracker_field': ['array','of','possible','data','columns']
DATA_CONTRACT = {
    'state_voter_id': ['VOTER_ID'],
    'name_first': ['FIRST_NAME'],
    'name_last': ['LAST_NAME'],
    'name_middle': ['MIDDLE_NAME'],

    'birthdate': ['BIRTH_DATE'],
    'party_affiliation': ['PARTY_CODE'],
    'registration_date': ['EFF_REGN_DATE'],
    'voter_status': ['STATUS'],
    'confidential': ['CONFIDENTIAL'],

    'physical_address_1': ['RES_ADDRESS_1'],
    'physical_address_2': ['RES_ADDRESS_2'],
    'physical_city': ['CITY'],
    'physical_state': ['STATE'],
    'physical_zip_code': ['ZIP_CODE'],

    'physical_house_number': ['HOUSE_NUM'],
    'physical_house_suffix': ['HOUSE_SUFFIX'],
    'physical_street_pre_direction': ['PRE_DIRECTION'],
    'physical_street_name': ['STREET_NAME'],
    'physical_street_suffix': ['STREET_TYPE'],
    'physical_street_post_direction': ['POST_DIRECTION'],
    'physical_unit_type': ['UNIT_TYPE'],
    'physical_unit_number': ['UNIT_NUM'],

    'county': ['COUNTY'],
    'precinct': ['PRECINCT'],
    'split': ['SPLIT'],

    'mail_address_1': ['EFF_ADDRESS_1'],
    'mail_address_2': ['EFF_ADDRESS_2'],
    'mail_address_3': ['EFF_ADDRESS_3'], # optional
    'mail_address_4': ['EFF_ADDRESS_4'], # optional
    'mail_city': ['EFF_CITY'],
    'mail_state': ['EFF_STATE'],
    'mail_zip_code': ['EFF_ZIP_CODE'],
}

dtype_mapping = {
    'state_voter_id': sqlalchemy.types.String(length=255),
    'file_date': sqlalchemy.types.Date(),
    'state': sqlalchemy.types.String(length=50),
    'name_first': sqlalchemy.types.String(length=100),
    'name_last': sqlalchemy.types.String(length=100),
    'name_middle': sqlalchemy.types.String(length=100),
    'birthdate': sqlalchemy.types.Date(),
    'age': sqlalchemy.types.Integer(),
    'party_affiliation': sqlalchemy.types.String(length=50),
    'registration_date': sqlalchemy.types.Date(),
    'voter_status': sqlalchemy.types.String(length=50),
    'confidential': sqlalchemy.types.String(length=50),
    'physical_id': sqlalchemy.types.String(length=255),
    'PropertyAddressFull': sqlalchemy.types.String(length=100),
    'PropertyAddressHouseNumber': sqlalchemy.types.String(length=50),
    'PropertyAddressStreetDirection': sqlalchemy.types.String(length=50),
    'PropertyAddressStreetName': sqlalchemy.types.String(length=100),
    'PropertyAddressStreetSuffix': sqlalchemy.types.String(length=50),
    'PropertyAddressCity': sqlalchemy.types.String(length=100),
    'PropertyAddressState': sqlalchemy.types.String(length=50),
    'PropertyAddressZIP': sqlalchemy.types.String(length=20),
    'PropertyAddressZIP4': sqlalchemy.types.String(length=20),
    'PropertyAddressCRRT': sqlalchemy.types.String(length=20),
    'PropertyLatitude': sqlalchemy.types.Float(),
    'PropertyLongitude': sqlalchemy.types.Float(),
    'precinct_link': sqlalchemy.types.String(length=255),
    'address_type': sqlalchemy.types.String(length=50),
    'results': sqlalchemy.types.String(length=50),
    'physical_address_1': sqlalchemy.types.String(length=100),
    'physical_address_2': sqlalchemy.types.String(length=100),
    'physical_city': sqlalchemy.types.String(length=100),
    'physical_state': sqlalchemy.types.String(length=50),
    'physical_zip_code': sqlalchemy.types.String(length=20),
    'physical_house_number': sqlalchemy.types.String(length=50),
    'physical_house_suffix': sqlalchemy.types.String(length=50),
    'physical_street_pre_direction': sqlalchemy.types.String(length=50),
    'physical_street_name': sqlalchemy.types.String(length=100),
    'physical_street_suffix': sqlalchemy.types.String(length=50),
    'physical_street_post_direction': sqlalchemy.types.String(length=50),
    'physical_unit_type': sqlalchemy.types.String(length=50),
    'physical_unit_number': sqlalchemy.types.String(length=50),
    'county': sqlalchemy.types.String(length=100),
    'precinct': sqlalchemy.types.String(length=100),
    'split': sqlalchemy.types.String(length=50),
    'mail_address_1': sqlalchemy.types.String(length=200),
    'mail_address_2': sqlalchemy.types.String(length=200),
    'mail_address_3': sqlalchemy.types.String(length=200),
    'mail_address_4': sqlalchemy.types.String(length=200),
    'mail_city': sqlalchemy.types.String(length=100),
    'mail_state': sqlalchemy.types.String(length=50),
    'mail_zip_code': sqlalchemy.types.String(length=20)
}

final_columns = [
    'state_voter_id',
    'file_date',
    'state',
    'name_first',
    'name_last',
    'name_middle',
    'birthdate',
    'age',
    'party_affiliation',
    'registration_date',
    'voter_status',
    'confidential',
    'physical_id',
    'PropertyAddressFull',
    'PropertyAddressHouseNumber',
    'PropertyAddressStreetDirection',
    'PropertyAddressStreetName',
    'PropertyAddressStreetSuffix',
    'PropertyAddressCity',
    'PropertyAddressState',
    'PropertyAddressZIP',
    'PropertyAddressZIP4',
    'PropertyAddressCRRT',
    'PropertyLatitude',
    'PropertyLongitude',
    'precinct_link',
    'address_type',
    'results',
    'physical_address_1',
    'physical_address_2',
    'physical_city',
    'physical_state',
    'physical_zip_code',
    'physical_house_number',
    'physical_house_suffix',
    'physical_street_pre_direction',
    'physical_street_name',
    'physical_street_suffix',
    'physical_street_post_direction',
    'physical_unit_type',
    'physical_unit_number',
    'county',
    'precinct',
    'split',
    'mail_address_1',
    'mail_address_2',
    'mail_address_3',
    'mail_address_4',
    'mail_city',
    'mail_state',
    'mail_zip_code'
]
