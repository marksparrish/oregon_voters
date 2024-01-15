STATE="oregon"
TABLENAME = 'voters'
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
