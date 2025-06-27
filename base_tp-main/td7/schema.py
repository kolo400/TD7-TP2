from typing import Optional, List
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import table as sql_table, column

from td7.custom_types import Records
from td7.database import Database

class Schema:
    def __init__(self):
        self.db = Database()        
    
    def get_voters(self, sample_n: Optional[int] = None) -> Records:
        query = "SELECT * FROM voters"
        if sample_n is not None:
            query += f" LIMIT {sample_n}"
        return self.db.run_select(query)

    def get_elections(self) -> Records:
        return self.db.run_select("SELECT * FROM elections")
    
    def get_votes(self) -> Records:
        return self.db.run_select("SELECT * FROM votes")
    
    def get_candidates(self) -> Records:
        return self.db.run_select("SELECT * FROM candidates")
    
    def get_polling_stations(self) -> Records:
        return self.db.run_select("SELECT * FROM polling_stations")
    
    def insert(self, records: Records, table: str):
        """Standard insert, will fail on duplicates."""
        self.db.run_insert(records, table)

    def upsert(self, records: Records, table_name: str, index_elements: List[str]) -> int:
        """
        Performs an "upsert" (INSERT ON CONFLICT DO NOTHING) for PostgreSQL
        and returns the number of rows actually inserted.

        :param records: List of dictionaries to insert.
        :param table_name: The name of the target table.
        :param index_elements: A list of column names that form the unique constraint.
        :return: The number of rows inserted (0 if all records were duplicates).
        """
        if not records:
            return 0

        table_obj = sql_table(table_name, *[column(c) for c in records[0].keys()])

        stmt = pg_insert(table_obj).values(records)
        stmt = stmt.on_conflict_do_nothing(
            index_elements=index_elements
        )
        
        with self.db.engine.connect() as conn:
            result = conn.execute(stmt)
            # For "ON CONFLICT DO NOTHING", this is 1 for an insert, 0 for a conflict.
            return result.rowcount

    def get_candidatos_por_eleccion(self) -> Records:
        return self.db.run_select("""SELECT dni_politico, id_eleccion FROM politico_eleccion_pertenece_partido""")

    def get_opciones_por_consulta(self) -> Records:
        return self.db.run_select("""SELECT id_eleccion, id_opcion FROM cp_tiene_opcion_respuesta""")