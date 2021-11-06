import pandas as pd
import io


def read_string_as_csv(csv_string: str, sep: str = ";") -> pd.DataFrame:
    """Input a csv as a string and return a dataframe."""
    return pd.read_csv(io.StringIO(csv_string), sep=sep)
