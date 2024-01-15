# data transformations raw data will be transformed into the required data.
# other transformations can be added in any of the raw, processed or final files as needed
# this can be completely ignored and all data transformations can be done the files
DATA_TRANSFORMATIONS = (
    {'column': ['birthdate'],
        'action': {
            'type':'cast_date',
            'column': ['birthdate'],
            'kwargs': {},
            'series': 0
        }
    },

    {'column': ['registration_date'],
        'action': {
            'type':'cast_date',
            'column': ['registration_date'],
            'kwargs': {},
            'series': 0
        }
    },

    {'column': ['confidential'],
        'action': {
            'type': 'confidential_voter',
            'column': ['physical_city'],
            'kwargs': {},
            'series': 0
        }
    },

    {'column': ['precinct_link'],
        'action': {
            'type': 'join_columns',
            'column': ['county', 'precinct', 'split'],
            'kwargs': {'sep': '_',},
            'series': 0
        }
    },

    {'column': ['gender'],
        'action': {
            'type': 'assume_gender',
            'column': ['name_first', 'name_middle'],
            'kwargs': {},
            'series': 0
        },
    },
)
