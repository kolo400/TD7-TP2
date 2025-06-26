from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
from td7.data_generator import DataGenerator
from td7.schema import Schema
from sqlalchemy import text

def get_table_count(schema, table_name):
    result = schema.db.connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
    return result.scalar()

def carga_inicial():
    generator = DataGenerator()
    schema = Schema()

    # 1. Centros de votación
    if get_table_count(schema, "centro_votacion") == 0:
        centros = generator.generate_centro_votacion(10)
        schema.insert(centros, "centro_votacion")
    else:
        centros = schema.db.run_select("SELECT * FROM centro_votacion")

    # 2. Elecciones base y tipos
    N_ELECCIONES = 3
    if get_table_count(schema, "eleccion") == 0:
        elecciones, elecciones_legislativas, consultas_populares = generator.generate_eleccion_typed(N_ELECCIONES)
        print("Elecciones base:", elecciones)
        print("Legislativas:", elecciones_legislativas)
        print("Consultas:", consultas_populares)
        schema.insert(elecciones, "eleccion")
        if elecciones_legislativas:
            schema.insert(elecciones_legislativas, "eleccion_legislativa")
        if consultas_populares:
            schema.insert(consultas_populares, "consulta_popular")
    else:
        elecciones = schema.db.run_select("SELECT * FROM eleccion")
        elecciones_legislativas = schema.db.run_select("SELECT * FROM eleccion_legislativa")
        consultas_populares = schema.db.run_select("SELECT * FROM consulta_popular")

    # 3. Mesas electorales
    if get_table_count(schema, "mesa_electoral") == 0:
        mesas = generator.generate_mesa_electoral(centros, elecciones)
        schema.insert(mesas, "mesa_electoral")
    else:
        mesas = schema.db.run_select("SELECT * FROM mesa_electoral")

    # 4. Máquinas de votación
    if get_table_count(schema, "maquina_votos") == 0:
        maquinas = generator.generate_maquina_votos(20)
        schema.insert(maquinas, "maquina_votos")
    else:
        maquinas = schema.db.run_select("SELECT * FROM maquina_votos")

    # 5. Relación mesa-maquina
    if get_table_count(schema, "mesa_utiliza_maquina") == 0:
        mesa_maquina = generator.generate_mesa_utiliza_maquina(mesas, maquinas)
        schema.insert(mesa_maquina, "mesa_utiliza_maquina")
    else:
        mesa_maquina = schema.db.run_select("SELECT * FROM mesa_utiliza_maquina")

    # 6. Electores
    if get_table_count(schema, "elector") == 0:
        electores = generator.generate_elector(100)
        schema.insert(electores, "elector")
    else:
        electores = schema.db.run_select("SELECT * FROM elector")

    # 7. Padrón electoral
    if get_table_count(schema, "padron_eleccion") == 0:
        padron = generator.generate_padron_eleccion(electores, mesas, elecciones)
        schema.insert(padron, "padron_eleccion")
    else:
        padron = schema.db.run_select("SELECT * FROM padron_eleccion")

    # 8. Partidos políticos
    if get_table_count(schema, "partido_politico") == 0:
        partidos = generator.generate_partido_politico(5)
        schema.insert(partidos, "partido_politico")
    else:
        partidos = schema.db.run_select("SELECT * FROM partido_politico")

    # 9. Políticos
    if get_table_count(schema, "politico") == 0:
        politicos = generator.generate_politico(20)
        schema.insert(politicos, "politico")
    else:
        politicos = schema.db.run_select("SELECT * FROM politico")

    # 10. Candidatos SOLO para elecciones legislativas
    if get_table_count(schema, "candidato") == 0 and elecciones_legislativas:
        candidatos = generator.generate_candidato(politicos, elecciones_legislativas)
        schema.insert(candidatos, "candidato")
        
    else:
        candidatos = schema.db.run_select("SELECT * FROM candidato")

    # 11. Relación político-elección-partido SOLO para elecciones legislativas
    if get_table_count(schema, "politico_eleccion_pertenece_partido") == 0 and elecciones_legislativas:
        politico_partido = generator.generate_politico_eleccion_pertenece_partido(candidatos, partidos)
        schema.insert(politico_partido, "politico_eleccion_pertenece_partido")
    else:
        politico_partido = schema.db.run_select("SELECT * FROM politico_eleccion_pertenece_partido")

    # 12. Opciones de respuesta SOLO para consultas populares
    if get_table_count(schema, "opcion_respuesta") == 0 and consultas_populares:
        opciones = generator.generate_opcion_respuesta(10)
        schema.insert(opciones, "opcion_respuesta")
    else:
        opciones = schema.db.run_select("SELECT * FROM opcion_respuesta")

    # 13. Relación consulta-opción SOLO para consultas populares
    if get_table_count(schema, "cp_tiene_opcion_respuesta") == 0 and consultas_populares and opciones:
        cp_opciones = generator.generate_cp_tiene_opcion_respuesta(consultas_populares, opciones)
        schema.insert(cp_opciones, "cp_tiene_opcion_respuesta")
    else:
        cp_opciones = schema.db.run_select("SELECT * FROM cp_tiene_opcion_respuesta")

with DAG(
    "carga_inicial_dag",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval=None,  # Solo manual
    catchup=False,
) as dag:
    inicio = EmptyOperator(task_id="inicio")
    carga = PythonOperator(
        task_id="carga_inicial",
        python_callable=carga_inicial,
    )
    fin = EmptyOperator(task_id="fin")
    inicio >> carga >> fin
