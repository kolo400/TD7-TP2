from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

from td7.data_generator import DataGenerator
from td7.schema import Schema

EVENTS_PER_DAY = 10_000

def generar_padron(**kwargs):
    gen    = DataGenerator()
    schema = Schema()

    electores  = schema.db.run_select("SELECT * FROM elector")
    mesas      = schema.db.run_select("SELECT * FROM mesa_utiliza_maquina")
    elecciones = schema.db.run_select("SELECT * FROM eleccion")

    registro = gen.generate_padron_eleccion(electores, mesas, elecciones)
    if registro:
        schema.insert([registro], "padron_eleccion")
    return registro

def decide_si_voto(**kwargs):
    ti     = kwargs['ti']
    padron = ti.xcom_pull(task_ids='generar_padron') or {}
    return 'insertar_voto' if padron.get('si_voto') else 'fin'

def generar_voto_task(**kwargs):
    gen    = DataGenerator()
    schema = Schema()
    ti     = kwargs['ti']

    padron         = schema.db.run_select("SELECT * FROM padron_eleccion WHERE si_voto = TRUE")
    mesas_maquinas = schema.db.run_select("SELECT * FROM mesa_utiliza_maquina")
    votos          = gen.generate_voto(padron, mesas_maquinas, len(padron))

    if not votos:
        ti.xcom_push(key='voto', value={})
        return

    voto = votos[0]
    tipo = voto.pop('tipo', None)
    schema.insert([voto], 'voto')
    voto['tipo'] = tipo
    ti.xcom_push(key='voto', value=voto)

def decide_tipo_voto(**kwargs):
    ti   = kwargs['ti']
    voto = ti.xcom_pull(task_ids='insertar_voto', key='voto') or {}
    tipo = voto.get('tipo')
    if tipo == 'legislativa':
        return 'insertar_legislativa'
    if tipo == 'consulta':
        return 'insertar_consulta'
    return 'fin'

# --------------- NUEVAS TAREAS PARA “ELIGE” ---------------

def insertar_voto_elige_candidato(**kwargs):
    ti         = kwargs['ti']
    voto       = ti.xcom_pull(task_ids='insertar_voto', key='voto')
    schema     = Schema()
    gen        = DataGenerator()
    candidatos = schema.get_candidatos_por_eleccion()
    vc         = gen.generate_voto_elige_candidato(voto, candidatos)
    if vc:
        schema.insert([vc], 'voto_elige_candidato')

def insertar_voto_elige_opcion(**kwargs):
    ti       = kwargs['ti']
    voto     = ti.xcom_pull(task_ids='insertar_voto', key='voto')
    schema   = Schema()
    gen      = DataGenerator()
    opciones = schema.get_opciones_por_consulta()
    vo       = gen.generate_voto_elige_opcion_respuesta(voto, opciones)
    if vo:
        schema.insert([vo], 'voto_elije_opcion_respuesta')

# --------------- TAREAS DE INSERCIÓN BASE ---------------

def insertar_legislativa(**kwargs):
    ti     = kwargs['ti']
    voto   = ti.xcom_pull(task_ids='insertar_voto', key='voto')
    schema = Schema()
    schema.insert([{
        'num_voto':    voto['num_voto'],
        'id_eleccion': voto['id_eleccion'],
    }], 'voto_eleccion_legislativa')

def insertar_consulta(**kwargs):
    ti     = kwargs['ti']
    voto   = ti.xcom_pull(task_ids='insertar_voto', key='voto')
    schema = Schema()
    schema.insert([{
        'num_voto':    voto['num_voto'],
        'id_eleccion': voto['id_eleccion'],
    }], 'voto_consulta_popular')


with DAG(
    dag_id='generar_voto_dag',
    start_date=pendulum.datetime(2025, 6, 1, tz='UTC'),
    schedule_interval='*/20 * * * *',
    catchup=False,
) as dag:

    inicio = EmptyOperator(task_id='inicio')

    # 1) Genero e inserto un padrón
    generar_padron_task = PythonOperator(
        task_id='generar_elector_padron',
        python_callable=generar_padron,
        provide_context=True
    )

    # 2) Branch: ¿votó o no?
    branch_si_voto = BranchPythonOperator(
        task_id='decidir_si_voto',
        python_callable=decide_si_voto,
        provide_context=True
    )

    # 3) Inserto el voto base
    insertar_voto_task = PythonOperator(
        task_id='insertar_voto',
        python_callable=generar_voto_task,
        provide_context=True
    )

    # 4) Branch según tipo de voto
    branch_tipo = BranchPythonOperator(
        task_id='decidir_tipo_voto',
        python_callable=decide_tipo_voto,
        provide_context=True
    )

    # --- Legislativa: base y luego “elige candidato” ---
    insertar_leg_task    = PythonOperator(
        task_id='insertar_legislativa',
        python_callable=insertar_legislativa,
        provide_context=True
    )
    elegir_candidato_task = PythonOperator(
        task_id='insertar_voto_elige_candidato',
        python_callable=insertar_voto_elige_candidato,
        provide_context=True
    )

    # --- Consulta: base y luego “elige opción” ---
    insertar_cons_task   = PythonOperator(
        task_id='insertar_consulta',
        python_callable=insertar_consulta,
        provide_context=True
    )
    elegir_opcion_task   = PythonOperator(
        task_id='insertar_voto_elige_opcion',
        python_callable=insertar_voto_elige_opcion,
        provide_context=True
    )

    fin = EmptyOperator(
        task_id='fin',
        trigger_rule='none_failed_min_one_success'
    )

    # Grafo
    inicio >> generar_padron_task >> branch_si_voto
    branch_si_voto >> insertar_voto_task >> branch_tipo
    branch_si_voto >> fin

    branch_tipo >> insertar_leg_task >> elegir_candidato_task >> fin
    branch_tipo >> insertar_cons_task >> elegir_opcion_task   >> fin