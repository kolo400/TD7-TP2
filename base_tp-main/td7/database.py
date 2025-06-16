import pandas as pd 
from sqlalchemy import create_engine

from td7.custom_types import Records
from td7.config import POSTGRES_CONN_STRING

class Database:
    """Class to interact with the database."""
    def __init__(self, conn_string = POSTGRES_CONN_STRING):
        self.engine = create_engine(conn_string)
        self.connection = self.engine.connect()

    def run_select(self, sql: str) -> Records:
        """Runs a select query and returns dict records.

        Parameters
        ----------
        sql : str
            SELECT query to run.

        Returns
        -------
        Records
        """
        dataframe = pd.read_sql(sql, self.connection)
        return dataframe.to_dict(orient="records")
    
    def run_insert(self, records: Records, table: str):
        """Runs an insert statement from data to table.

        Parameters
        ----------
        records : Records
            Data to insert.
        table : str
            Table name to insert.
        """
        pd.DataFrame(records).to_sql(table, self.connection, if_exists="append", index=False)

    def __del__(self):
        self.connection.close()
