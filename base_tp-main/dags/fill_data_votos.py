from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import datetime
from td7.data_generator import DataGenerator
from td7.schema import Schema
import random

EVENTS_PER_DAY = 10_000

def generar_voto(**kwargs):
    generator = DataGenerator()
    schema = Schema()
    padron = schema.db.run_select("SELECT * FROM padron_eleccion WHERE si_voto = TRUE")
    mesas_maquinas = schema.db.run_select("SELECT * FROM mesa_utiliza_maquina")
    votos = generator.generate_voto(padron, mesas_maquinas, len(padron))
    if votos:
        voto = votos[0]
        # Guardá el tipo para el branch, pero no lo insertes en la tabla
        tipo = voto.pop("tipo", None)
        schema.insert([voto], "voto")
        # Pasá el tipo por XCom para el branch
        voto["tipo"] = tipo
        kwargs['ti'].xcom_push(key='voto', value=voto)
        marcar_elector_como_voto(schema, voto["dni_elector"], voto["id_eleccion"])
    else:
        kwargs['ti'].xcom_push(key='voto', value={})

def elegir_tipo_voto(**kwargs):
    voto = kwargs['ti'].xcom_pull(key='voto', task_ids='insertar_voto')
    tipo = voto["tipo"]  # por ejemplo, "legislativa" o "consulta"
    if tipo == "legislativa":
        return "insertar_legislativa"
    elif tipo == "consulta":
        return "insertar_consulta"
    else:
        return "fin"

def insertar_legislativa(**kwargs):
    voto = kwargs['ti'].xcom_pull(key='voto', task_ids='insertar_voto')
    schema = Schema()
    # Solo los campos que existen en la tabla
    voto_legislativa = {
        "num_voto": voto["num_voto"],
        "id_eleccion": voto["id_eleccion"]
    }
    schema.insert([voto_legislativa], "voto_eleccion_legislativa")
    # Lo mismo para voto_elige_candidato, si corresponde
    # ...

def insertar_consulta(**kwargs):
    voto = kwargs['ti'].xcom_pull(key='voto', task_ids='insertar_voto')
    schema = Schema()
    # Solo los campos que existen en la tabla
    voto_consulta = {
        "num_voto": voto["num_voto"],
        "id_eleccion": voto["id_eleccion"]
    }
    schema.insert([voto_consulta], "voto_consulta_popular")
    # Lo mismo para voto_elige_opcion_respuesta, si corresponde
    # ...

def marcar_elector_como_voto(schema, dni_elector, id_eleccion):
    schema.db.connection.execute(
        f\"\"\"UPDATE padron_eleccion
        SET si_voto = TRUE
        WHERE dni_elector = '{dni_elector}' AND id_eleccion = '{id_eleccion}'\"\"\"
    )

def generate_padron_eleccion(self, electores: Records, mesas: Records) -> Records:
    padron = []
    for elector in electores:
        mesa = random.choice(mesas)
        padron.append({
            "dni_elector": elector["dni"],
            "id_eleccion": mesa["id_eleccion"],
            "nro_mesa": mesa["nro_mesa"],
            "id_centro": mesa["id_centro"],
            "si_voto": random.random() < 0.8  # 80% de probabilidad de que sea True
        })
    return padron

with DAG(
    "generar_voto_dag",
    start_date=pendulum.datetime(2024, 6, 1, tz="UTC"),
    schedule_interval="*/20 * * * * *",  # cada 20 segundos
    catchup=False,
) as dag:
    
    inicio = EmptyOperator(task_id="inicio")

    insertar_voto = PythonOperator(
        task_id="insertar_voto",
        python_callable=generar_voto,
    )

    decidir_tipo = BranchPythonOperator(
        task_id="decidir_tipo_voto",
        python_callable=elegir_tipo_voto,
    )

    insertar_legislativa = PythonOperator(
        task_id="insertar_legislativa",
        python_callable=insertar_legislativa,
    )

    insertar_consulta = PythonOperator(
        task_id="insertar_consulta",
        python_callable=insertar_consulta,
    )

    fin = EmptyOperator(task_id="fin", trigger_rule="none_failed_min_one_success")

    inicio >> insertar_voto >> decidir_tipo
    decidir_tipo >> insertar_legislativa >> fin
    decidir_tipo >> insertar_consulta >> fin
