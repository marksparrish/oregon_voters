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
