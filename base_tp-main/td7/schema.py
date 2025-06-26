from typing import Optional

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
        self.db.run_insert(records, table)

    def get_candidatos_por_eleccion(self) -> Records:
        return self.db.run_select("""SELECT dni_politico, id_eleccion FROM politico_eleccion_pertenece_partido""")

    def get_opciones_por_consulta(self) -> Records:
        return self.db.run_select("""SELECT id_eleccion, id_opcion FROM cp_tiene_opcion_respuesta""")