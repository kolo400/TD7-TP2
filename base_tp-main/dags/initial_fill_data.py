from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def check_initial_data(**context):
    """Verifica si los datos base ya fueron cargados."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Verificar si ya existen datos en alguna tabla base
    existing_data = pg_hook.get_first("""
        SELECT COUNT(*) 
        FROM electores
    """)
    
    if existing_data[0] > 0:
        raise AirflowSkipException("Los datos base ya fueron cargados previamente")

def generate_base_data(**context):
    """Genera datos base del sistema."""
    generator = DataGenerator()
    
    # Generar datos base
    electores = generator.generate_electores(1000)
    maquinas = generator.generate_maquinas_votos(50)
    centros = generator.generate_centros_votacion(10)
    elecciones = generator.generate_elecciones(5)
    elecciones_legislativas = generator.generate_elecciones_legislativas(elecciones)
    consultas_populares = generator.generate_consultas_populares(elecciones)
    partidos = generator.generate_partidos_politicos(5)
    candidatos = generator.generate_candidatos(politicos, elecciones_legislativas)
    
    # Generar mesas y padrones
    mesas = generator.generate_mesas_electorales(centros, elecciones)
    mesas_maquinas = generator.generate_mesas_utiliza_maquina(mesas, maquinas)
    padron = generator.generate_padron_eleccion(electores, mesas)
    
    # Guardar en XCom
    context['task_instance'].xcom_push(key='base_data', value={
        'electores': electores,
        'maquinas': maquinas,
        'centros': centros,
        'elecciones': elecciones,
        'elecciones_legislativas': elecciones_legislativas,
        'consultas_populares': consultas_populares,
        'partidos': partidos,
        'candidatos': candidatos,
        'mesas': mesas,
        'mesas_maquinas': mesas_maquinas,
        'padron': padron
    })

def load_base_data(**context):
    """Carga los datos base en la base de datos."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    base_data = context['task_instance'].xcom_pull(task_ids='generate_base_data', key='base_data')
    
    # Cargar cada conjunto de datos
    for table_name, data in base_data.items():
        if data:
            df = pd.DataFrame(data)
            pg_hook.insert_rows(table=table_name, rows=df.values.tolist())

with DAG(
    'initial_data_load',
    default_args=default_args,
    description='Carga inicial de datos base del sistema de votación',
    schedule_interval=None,  # No se ejecuta automáticamente
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['voting', 'initial_load'],
) as dag:

    # Verificar si ya existen datos
    check_data = PythonOperator(
        task_id='check_initial_data',
        python_callable=check_initial_data,
    )

    # Generar datos base
    generate_base = PythonOperator(
        task_id='generate_base_data',
        python_callable=generate_base_data,
    )

    # Cargar datos base
    load_base = PythonOperator(
        task_id='load_base_data',
        python_callable=load_base_data,
    )

    # Definir el flujo
    check_data >> generate_base >> load_base



    def process_center_data(center_id, **context):
    """Procesa los datos para un centro de votación específico."""
    generator = DataGenerator()
    
    # Recuperar datos base de la base de datos
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Obtener mesas y padrones existentes
    mesas = pg_hook.get_pandas_df("SELECT * FROM mesa_electoral WHERE id_centro = %s", [center_id])
    padron = pg_hook.get_pandas_df("SELECT * FROM padron_eleccion WHERE id_mesa IN (SELECT id FROM mesa_electoral WHERE id_centro = %s)", [center_id])
    
    # Generar votos para este centro
    votos = generator.generate_votos(padron, mesas, 50)  # 50 votos por centro
    
    # Guardar votos en XCom
    context['task_instance'].xcom_push(key=f'votos_center_{center_id}', value=votos)

def process_center_results(center_id, **context):
    """Procesa los resultados de votación para un centro específico."""
    generator = DataGenerator()
    
    # Recuperar datos necesarios
    votos = context['task_instance'].xcom_pull(task_ids=f'process_center_{center_id}', key=f'votos_center_{center_id}')
    
    # Obtener candidatos y opciones de la base de datos
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    candidatos = pg_hook.get_pandas_df("SELECT * FROM candidato")
    cp_opciones = pg_hook.get_pandas_df("SELECT * FROM cp_tiene_opcion_respuesta")
    
    # Generar resultados
    votos_legislativos = generator.generate_votos_eleccion_legislativa(votos, candidatos)
    votos_consultas = generator.generate_votos_consulta_popular(votos, cp_opciones)
    
    # Guardar resultados en XCom
    context['task_instance'].xcom_push(key=f'votos_legislativos_center_{center_id}', value=votos_legislativos)
    context['task_instance'].xcom_push(key=f'votos_consultas_center_{center_id}', value=votos_consultas)