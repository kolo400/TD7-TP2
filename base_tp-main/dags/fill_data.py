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

def generate_base_data(**context):
    """Genera datos base del sistema."""
    generator = DataGenerator()
    
    # Generar datos base
    electores = generator.generate_electores(1000)
    integrantes = generator.generate_integrantes(100)
    tecnicos = generator.generate_tecnicos(integrantes, 20)
    maquinas = generator.generate_maquinas_votos(50)
    centros = generator.generate_centros_votacion(10)
    elecciones = generator.generate_elecciones(5)

    # Guardar en XCom para uso posterior
    context['task_instance'].xcom_push(key='electores', value=electores)
    context['task_instance'].xcom_push(key='integrantes', value=integrantes)
    context['task_instance'].xcom_push(key='tecnicos', value=tecnicos)
    context['task_instance'].xcom_push(key='maquinas', value=maquinas)
    context['task_instance'].xcom_push(key='centros', value=centros)
    context['task_instance'].xcom_push(key='elecciones', value=elecciones)

def generate_election_data(**context):
    """Genera datos de elecciones y partidos."""
    generator = DataGenerator()
    elecciones = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='elecciones')
    
    # Generar elecciones legislativas
    elecciones_legislativas = generator.generate_elecciones_legislativas(elecciones)
    
    # Generar datos de partidos y candidatos
    partidos = generator.generate_partidos_politicos(5)
    politicos = generator.generate_politicos(20)
    candidatos = generator.generate_candidatos(politicos, elecciones_legislativas)
    politico_eleccion_partido = generator.generate_politico_eleccion_partido(candidatos, partidos)

    # Generar consultas populares
    consultas_populares = generator.generate_consultas_populares(elecciones)
    opciones = generator.generate_opciones_respuesta(10)
    cp_opciones = generator.generate_cp_tiene_opcion_respuesta(consultas_populares, opciones)

    # Guardar en XCom
    context['task_instance'].xcom_push(key='elecciones_legislativas', value=elecciones_legislativas)
    context['task_instance'].xcom_push(key='partidos', value=partidos)
    context['task_instance'].xcom_push(key='politicos', value=politicos)
    context['task_instance'].xcom_push(key='candidatos', value=candidatos)
    context['task_instance'].xcom_push(key='politico_eleccion_partido', value=politico_eleccion_partido)
    context['task_instance'].xcom_push(key='consultas_populares', value=consultas_populares)
    context['task_instance'].xcom_push(key='opciones', value=opciones)
    context['task_instance'].xcom_push(key='cp_opciones', value=cp_opciones)

def process_center_data(center_id, **context):
    """Procesa los datos para un centro de votación específico."""
    generator = DataGenerator()
    
    # Recuperar datos base
    electores = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='electores')
    integrantes = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='integrantes')
    tecnicos = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='tecnicos')
    maquinas = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='maquinas')
    centros = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='centros')
    elecciones = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='elecciones')
    
    # Filtrar datos para este centro
    centro = next(c for c in centros if c['id'] == center_id)
    maquinas_centro = [m for m in maquinas if m['id_centro'] == center_id]
    
    # Generar mesas para este centro
    mesas = generator.generate_mesas_electorales([centro], elecciones, tecnicos, integrantes)
    mesas_maquinas = generator.generate_mesas_utiliza_maquina(mesas, maquinas_centro)
    
    # Generar padrón para este centro
    padron = generator.generate_padron_eleccion(electores, mesas)
    
    # Generar fiscales para este centro
    fiscales = generator.generate_fiscales(integrantes, 4)  # 4 fiscales por centro
    mesa_fiscal = generator.generate_mesa_fiscal(fiscales, mesas)
    fiscal_partido = generator.generate_fiscal_partido(fiscales, 
        context['task_instance'].xcom_pull(task_ids='generate_election_data', key='partidos'))
    
    # Generar votos para este centro
    votos = generator.generate_votos(padron, mesas_maquinas, 50)  # 50 votos por centro
    
    # Guardar datos del centro en XCom
    context['task_instance'].xcom_push(key=f'mesas_center_{center_id}', value=mesas)
    context['task_instance'].xcom_push(key=f'mesas_maquinas_center_{center_id}', value=mesas_maquinas)
    context['task_instance'].xcom_push(key=f'padron_center_{center_id}', value=padron)
    context['task_instance'].xcom_push(key=f'fiscales_center_{center_id}', value=fiscales)
    context['task_instance'].xcom_push(key=f'mesa_fiscal_center_{center_id}', value=mesa_fiscal)
    context['task_instance'].xcom_push(key=f'votos_center_{center_id}', value=votos)

def process_center_results(center_id, **context):
    """Procesa los resultados de votación para un centro específico."""
    generator = DataGenerator()
    
    # Recuperar datos necesarios
    votos = context['task_instance'].xcom_pull(task_ids=f'process_center_{center_id}', key=f'votos_center_{center_id}')
    candidatos = context['task_instance'].xcom_pull(task_ids='generate_election_data', key='candidatos')
    cp_opciones = context['task_instance'].xcom_pull(task_ids='generate_election_data', key='cp_opciones')
    
    # Generar resultados de elecciones legislativas
    votos_legislativos = generator.generate_votos_eleccion_legislativa(votos, candidatos)
    voto_elige_candidato = generator.generate_voto_elige_candidato(votos_legislativos, candidatos)
    
    # Generar resultados de consultas populares
    votos_consultas = generator.generate_votos_consulta_popular(votos, 
        context['task_instance'].xcom_pull(task_ids='generate_election_data', key='consultas_populares'))
    voto_elige_opcion = generator.generate_voto_elige_opcion_respuesta(votos_consultas, cp_opciones)
    
    # Guardar resultados en XCom
    context['task_instance'].xcom_push(key=f'votos_legislativos_center_{center_id}', value=votos_legislativos)
    context['task_instance'].xcom_push(key=f'voto_elige_candidato_center_{center_id}', value=voto_elige_candidato)
    context['task_instance'].xcom_push(key=f'votos_consultas_center_{center_id}', value=votos_consultas)
    context['task_instance'].xcom_push(key=f'voto_elige_opcion_center_{center_id}', value=voto_elige_opcion)

def load_center_data(center_id, **context):
    """Carga los datos de un centro específico en la base de datos."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    task_instance = context['task_instance']
    
    def load_data(table_name, data):
        if data:
            df = pd.DataFrame(data)
            pg_hook.insert_rows(table=table_name, rows=df.values.tolist())
    
    # Cargar datos del centro
    load_data('mesa', task_instance.xcom_pull(task_ids=f'process_center_{center_id}', key=f'mesas_center_{center_id}'))
    load_data('mesa_utiliza_maquina', task_instance.xcom_pull(task_ids=f'process_center_{center_id}', key=f'mesas_maquinas_center_{center_id}'))
    load_data('padron_eleccion', task_instance.xcom_pull(task_ids=f'process_center_{center_id}', key=f'padron_center_{center_id}'))
    load_data('fiscal', task_instance.xcom_pull(task_ids=f'process_center_{center_id}', key=f'fiscales_center_{center_id}'))
    load_data('mesa_fiscal', task_instance.xcom_pull(task_ids=f'process_center_{center_id}', key=f'mesa_fiscal_center_{center_id}'))
    load_data('voto', task_instance.xcom_pull(task_ids=f'process_center_{center_id}', key=f'votos_center_{center_id}'))
    
    # Cargar resultados del centro
    load_data('voto_eleccion_legislativa', task_instance.xcom_pull(task_ids=f'process_center_results_{center_id}', key=f'votos_legislativos_center_{center_id}'))
    load_data('voto_consulta_popular', task_instance.xcom_pull(task_ids=f'process_center_results_{center_id}', key=f'votos_consultas_center_{center_id}'))
    load_data('voto_elige_candidato', task_instance.xcom_pull(task_ids=f'process_center_results_{center_id}', key=f'voto_elige_candidato_center_{center_id}'))
    load_data('voto_elige_opcion_respuesta', task_instance.xcom_pull(task_ids=f'process_center_results_{center_id}', key=f'voto_elige_opcion_center_{center_id}'))

def load_common_data(**context):
    """Carga los datos comunes en la base de datos."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    task_instance = context['task_instance']
    
    def load_data(table_name, data):
        if data:
            df = pd.DataFrame(data)
            pg_hook.insert_rows(table=table_name, rows=df.values.tolist())
    
    # Cargar datos base
    load_data('elector', task_instance.xcom_pull(task_ids='generate_base_data', key='electores'))
    load_data('integrante', task_instance.xcom_pull(task_ids='generate_base_data', key='integrantes'))
    load_data('tecnico', task_instance.xcom_pull(task_ids='generate_base_data', key='tecnicos'))
    load_data('maquina_voto', task_instance.xcom_pull(task_ids='generate_base_data', key='maquinas'))
    load_data('centro_votacion', task_instance.xcom_pull(task_ids='generate_base_data', key='centros'))
    load_data('eleccion', task_instance.xcom_pull(task_ids='generate_base_data', key='elecciones'))
    
    # Cargar datos de elecciones
    load_data('eleccion_legislativa', task_instance.xcom_pull(task_ids='generate_election_data', key='elecciones_legislativas'))
    load_data('partido_politico', task_instance.xcom_pull(task_ids='generate_election_data', key='partidos'))
    load_data('politico', task_instance.xcom_pull(task_ids='generate_election_data', key='politicos'))
    load_data('candidato', task_instance.xcom_pull(task_ids='generate_election_data', key='candidatos'))
    load_data('politico_eleccion_partido', task_instance.xcom_pull(task_ids='generate_election_data', key='politico_eleccion_partido'))
    load_data('consulta_popular', task_instance.xcom_pull(task_ids='generate_election_data', key='consultas_populares'))
    load_data('opcion_respuesta', task_instance.xcom_pull(task_ids='generate_election_data', key='opciones'))
    load_data('cp_tiene_opcion_respuesta', task_instance.xcom_pull(task_ids='generate_election_data', key='cp_opciones'))

with DAG(
    'fill_voting_data',
    default_args=default_args,
    description='Genera y carga datos sintéticos para el sistema de votación por centro',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['voting', 'data_generation'],
) as dag:

    # Tarea para crear las tablas
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id='postgres_default',
        sql='sql/create_tables.sql'
    )

    # Tarea para generar datos base
    generate_base = PythonOperator(
        task_id='generate_base_data',
        python_callable=generate_base_data,
    )

    # Tarea para generar datos de elecciones
    generate_elections = PythonOperator(
        task_id='generate_election_data',
        python_callable=generate_election_data,
    )

    # Tarea para cargar datos comunes
    load_common = PythonOperator(
        task_id='load_common_data',
        python_callable=load_common_data,
    )

    # Crear tareas para cada centro de votación
    center_tasks = []
    for center_id in range(1, 11):  # 10 centros de votación
        # Procesar datos del centro
        process_center = PythonOperator(
            task_id=f'process_center_{center_id}',
            python_callable=process_center_data,
            op_kwargs={'center_id': center_id},
        )
        
        # Procesar resultados del centro
        process_results = PythonOperator(
            task_id=f'process_center_results_{center_id}',
            python_callable=process_center_results,
            op_kwargs={'center_id': center_id},
        )
        
        # Cargar datos del centro
        load_center = PythonOperator(
            task_id=f'load_center_data_{center_id}',
            python_callable=load_center_data,
            op_kwargs={'center_id': center_id},
        )
        
        # Definir dependencias del centro
        process_center >> process_results >> load_center
        center_tasks.append(process_center)

    # Definir el orden de las tareas
    create_tables >> generate_base >> generate_elections >> load_common >> center_tasks
