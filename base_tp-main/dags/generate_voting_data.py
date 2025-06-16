from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from td7.data_generator import DataGenerator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_and_load_data(**context):
    """Genera datos sintéticos y los carga en la base de datos."""
    generator = DataGenerator()
    
    # Generar datos base
    electores = generator.generate_electores(1000)
    integrantes = generator.generate_integrantes(100)
    tecnicos = generator.generate_tecnicos(integrantes, 20)
    maquinas = generator.generate_maquinas_votos(50)
    centros = generator.generate_centros_votacion(10)
    elecciones = generator.generate_elecciones(5)

    # Generar tipos específicos de elecciones
    elecciones_legislativas = generator.generate_elecciones_legislativas(elecciones)
    consultas_populares = generator.generate_consultas_populares(elecciones)

    # Generar mesas y sus relaciones
    mesas = generator.generate_mesas_electorales(centros, elecciones, tecnicos, integrantes)
    mesas_maquinas = generator.generate_mesas_utiliza_maquina(mesas, maquinas)

    # Generar padrón
    padron = generator.generate_padron_eleccion(electores, mesas)

    # Generar datos de partidos y candidatos
    partidos = generator.generate_partidos_politicos(5)
    politicos = generator.generate_politicos(20)
    candidatos = generator.generate_candidatos(politicos, elecciones_legislativas)
    politico_eleccion_partido = generator.generate_politico_eleccion_partido(candidatos, partidos)

    # Generar datos de fiscales
    fiscales = generator.generate_fiscales(integrantes, 40)
    mesa_fiscal = generator.generate_mesa_fiscal(fiscales, mesas)
    fiscal_partido = generator.generate_fiscal_partido(fiscales, partidos)

    # Generar datos de consultas populares
    opciones = generator.generate_opciones_respuesta(10)
    cp_opciones = generator.generate_cp_tiene_opcion_respuesta(consultas_populares, opciones)

    # Generar votos
    votos = generator.generate_votos(padron, mesas_maquinas, 500)
    votos_legislativos = generator.generate_votos_eleccion_legislativa(votos, candidatos)
    votos_consultas = generator.generate_votos_consulta_popular(votos, consultas_populares)
    voto_elige_candidato = generator.generate_voto_elige_candidato(votos_legislativos, candidatos)
    voto_elige_opcion = generator.generate_voto_elige_opcion_respuesta(votos_consultas, cp_opciones)

    # Cargar datos en la base de datos
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Función auxiliar para cargar datos
    def load_data(table_name, data):
        if data:
            df = pd.DataFrame(data)
            pg_hook.insert_rows(table=table_name, rows=df.values.tolist())

    # Cargar todos los datos
    load_data('elector', electores)
    load_data('integrante', integrantes)
    load_data('tecnico', tecnicos)
    load_data('maquina_voto', maquinas)
    load_data('centro_votacion', centros)
    load_data('eleccion', elecciones)
    load_data('eleccion_legislativa', elecciones_legislativas)
    load_data('consulta_popular', consultas_populares)
    load_data('mesa', mesas)
    load_data('mesa_utiliza_maquina', mesas_maquinas)
    load_data('padron_eleccion', padron)
    load_data('partido_politico', partidos)
    load_data('politico', politicos)
    load_data('candidato', candidatos)
    load_data('politico_eleccion_partido', politico_eleccion_partido)
    load_data('fiscal', fiscales)
    load_data('mesa_fiscal', mesa_fiscal)
    load_data('fiscal_partido', fiscal_partido)
    load_data('opcion_respuesta', opciones)
    load_data('cp_tiene_opcion_respuesta', cp_opciones)
    load_data('voto', votos)
    load_data('voto_eleccion_legislativa', votos_legislativos)
    load_data('voto_consulta_popular', votos_consultas)
    load_data('voto_elige_candidato', voto_elige_candidato)
    load_data('voto_elige_opcion_respuesta', voto_elige_opcion)

with DAG(
    'generate_voting_data',
    default_args=default_args,
    description='Genera y carga datos sintéticos para el sistema de votación',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['voting', 'data_generation'],
) as dag:

    # Tarea para crear las tablas si no existen
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql='sql/create_tables.sql'
    )

    # Tarea para generar y cargar los datos
    generate_data = PythonOperator(
        task_id='generate_and_load_data',
        python_callable=generate_and_load_data,
    )

    # Definir el orden de las tareas
    create_tables >> generate_data 