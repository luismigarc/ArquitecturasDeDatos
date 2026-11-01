from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule


BUCKET = "data-lake-arquitecturas-luismi-2026"
REGION = "us-east-2"
GLUE_CRAWLER = "data-lake-noticias-crawler"


default_args = {
    "owner": "luismi",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def decidir_ingesta(**context):
    """
    Ejecucion normal:
    {"reprocess_from_bronze": false}

    Reprocesamiento desde Bronze:
    {"reprocess_from_bronze": true}
    """
    conf = context["dag_run"].conf or {}
    reprocess = conf.get("reprocess_from_bronze", False)

    if isinstance(reprocess, str):
        reprocess = reprocess.lower() == "true"

    if reprocess:
        return "omitir_ingesta_rss"

    return "ingest_rss"


def ejecutar_glue_crawler():
    glue = boto3.client("glue", region_name=REGION)

    crawler = glue.get_crawler(Name=GLUE_CRAWLER)["Crawler"]
    estado = crawler["State"]

    if estado == "RUNNING":
        print(f"El crawler {GLUE_CRAWLER} ya esta ejecutandose.")
        return

    glue.start_crawler(Name=GLUE_CRAWLER)
    print(f"Crawler iniciado: {GLUE_CRAWLER}")


def validar_athena():
    athena = boto3.client("athena", region_name=REGION)

    query = """
    SELECT COUNT(*) AS total_noticias
    FROM data_lake_noticias.news
    WHERE processing_date = (
        SELECT MAX(processing_date)
        FROM data_lake_noticias.news
    )
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": "data_lake_noticias"
        },
        ResultConfiguration={
            "OutputLocation": f"s3://{BUCKET}/athena-results/"
        },
    )

    query_execution_id = response["QueryExecutionId"]

    print(f"Athena QueryExecutionId: {query_execution_id}")
    print("Consulta de validacion enviada correctamente.")


with DAG(
    dag_id="data_lake_noticias",
    description="Pipeline Data Lake El Tiempo: Bronze, Silver, Comprehend, Gold, Glue y Athena",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-lake", "el-tiempo", "arquitectura-datos"],
) as dag:

    decidir_fuente = BranchPythonOperator(
        task_id="decidir_fuente",
        python_callable=decidir_ingesta,
    )

    ingest_rss = BashOperator(
        task_id="ingest_rss",
        bash_command="python /opt/airflow/scripts/ingest_rss.py",
    )

    omitir_ingesta_rss = EmptyOperator(
        task_id="omitir_ingesta_rss",
    )

    process_silver = BashOperator(
        task_id="process_silver",
        bash_command="python /opt/airflow/scripts/process_silver.py",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    enrich_comprehend = BashOperator(
        task_id="enrich_comprehend",
        bash_command="python /opt/airflow/scripts/enrich_comprehend.py",
    )

    process_gold = BashOperator(
        task_id="process_gold",
        bash_command="python /opt/airflow/scripts/process_gold.py",
    )

    actualizar_glue = PythonOperator(
        task_id="actualizar_glue",
        python_callable=ejecutar_glue_crawler,
    )

    validar_con_athena = PythonOperator(
        task_id="validar_con_athena",
        python_callable=validar_athena,
    )

    fin_pipeline = EmptyOperator(
        task_id="fin_pipeline",
    )

    decidir_fuente >> [ingest_rss, omitir_ingesta_rss]

    [ingest_rss, omitir_ingesta_rss] >> process_silver

    process_silver >> enrich_comprehend >> process_gold
    process_gold >> actualizar_glue >> validar_con_athena >> fin_pipeline
