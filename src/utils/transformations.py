import re
import pandas as pd

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

def truncate_words(address, max_length=10):
    """
    Truncates each word in a given address string to a specified maximum length.

    This function takes an address string, splits it into individual words, and then truncates each word to ensure that it does not exceed the specified maximum length. The truncated words are then joined back together into a single string, separated by spaces. This can be useful for ensuring that no word in an address exceeds a certain length, for example, when displaying addresses in a UI with limited space.

    Parameters:
    - address (str): The address string to be truncated. It is assumed to be a sequence of words separated by spaces.
    - max_length (int, optional): The maximum length for each word in the address. Defaults to 10 characters.

    Returns:
    - str: A new string where each word from the original address has been truncated to the specified maximum length.

    Example:
    >>> truncate_words("1234 Long Street Name, Big City", max_length=5)
    '1234 Long Stree Name, Big City'

    Note:
    - If `max_length` is set to a value less than 1, words will not be truncated, potentially leading to unexpected results.
    - Punctuation is considered part of a word, so it will count towards the word's total length when truncating.
    """
    truncated_words = [word[:max_length] for word in address.split()]
    return ' '.join(truncated_words)
