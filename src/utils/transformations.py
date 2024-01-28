import re
import pandas as pd
# Initialize pandarallel
from pandarallel import pandarallel

def initialize_pandarallel():
    pandarallel.initialize(progress_bar=True, use_memory_fs=False)

def join_columns(df, columns, new_column_name, sep=' ') -> pd.DataFrame:
    """
    Join multiple columns into a single column
    """
    df[new_column_name] = df[columns].parallel_apply(lambda x: sep.join(x.dropna().astype(str)), axis=1)
    return df

def mark_homeless_addresses(df, tests):
    """
    Mark rows as 'Homeless' based on various string tests provided in a list of tuples.

    :param df: DataFrame to process
    :param tests: List of tuples where each tuple contains (column, test_type, condition)
    :return: Modified DataFrame
    """

    # Initialize an empty mask
    mask = pd.Series(False, index=df.index)

    # Apply tests from the list
    columns = ['physical_address_1', 'physical_address_2']
    for column in columns:
        for test_type, condition in tests:
            if test_type == 'contains':
                mask |= df[column].str.contains(condition, na=False, flags=re.IGNORECASE)
            elif test_type == 'startswith':
                mask |= df[column].str.startswith(condition, na=False)

    # Mark rows as 'Homeless'
    df.loc[mask, 'results'] = 'Homeless'
    return df

def convert_date_format(date_str):
    """
    Convert a date string from 'mm-dd-yyyy' format to 'yyyy-mm-dd' format.

    Parameters:
    date_str (str): A date string in 'mm-dd-yyyy' format.

    Returns:
    str: The date string in 'yyyy-mm-dd' format.
    """
    try:
        month, day, year = date_str.split('-')
        return f'{year}-{month}-{day}'
    except ValueError:
        # Handle the case where the input does not match the expected format
        return None  # or raise an exception, or handle it in another way
