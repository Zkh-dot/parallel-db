import pandas as pd
import sqlalchemy
from datetime import date, datetime


class methods:
    @staticmethod
    def to_date(table: pd.DataFrame, date: str, mapping):
        """
        Converts a column in a pandas DataFrame to datetime format.

        Args:
            table (pd.DataFrame): The DataFrame containing the column to be converted.
            date (str): The name of the column to be converted.
            mapping: A dictionary mapping column names to their respective data types.

        Returns:
            pd.DataFrame: The modified DataFrame with the column converted to datetime format.
            mapping: The updated mapping dictionary.
        """
        if date in mapping:
            for i, row in enumerate(table[date]):
                if table[date][i] == None:
                    continue
                table[date][i] = datetime.strptime(table[date][i], '%Y-%m-%d')
            mapping[date] = sqlalchemy.types.Date
        else:
            pass

        return table, mapping
