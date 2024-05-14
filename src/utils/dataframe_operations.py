import pandas as pd

def validate_dataframe(df, data_contract):
    print("Validating...", end=" ")

    vdf = pd.DataFrame()

    for key, possible_headers in data_contract.items():
        matching_columns = df.columns[df.columns.isin(possible_headers)]

        if len(matching_columns) > 0:
            vdf[key] = df[matching_columns[0]].str.strip()
        else:
            raise ValueError(f"Broken Contract! Missing columns for {key}: {possible_headers}")

    print("Done")
    return vdf.reset_index(drop=True)

def diff_dataframe(df_new, df_existing, on_columns):
    """
    Insert new records into the database.

    Parameters:
    df_new (pd.DataFrame): DataFrame with new records
    df_existing (pd.DataFrame): DataFrame with existing records
    session (sqlalchemy.orm.session.Session): SQLAlchemy session
    """
    # Merge DataFrames to find new records
    df_merge = pd.merge(df_new, df_existing, on=on_columns, how='left', indicator=True)
    df_diff = df_merge[df_merge['_merge'] == 'left_only'].drop(columns=['_merge'])
    return df_diff
