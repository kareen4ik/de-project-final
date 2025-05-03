import sys
import os
sys.path.insert(0, "/opt/airflow/src")

import logging
import pendulum
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from lib.pg_connect import PgConnect
from stg_transactions_loader import StgTransactionsLoader
from stg_currencies_loader import StgCurrenciesLoader
from dwh_global_metrics_loader import DWHGlobalMetricsLoader

load_dotenv('/opt/airflow/.env')

log = logging.getLogger(__name__)

VERTICA_CONFIG = {
    'host':     os.getenv('VERTICA_HOST'),
    'port':     int(os.getenv('VERTICA_PORT', 5433)),
    'user':     os.getenv('VERTICA_USER'),
    'password': os.getenv('VERTICA_PASSWORD'),
    'database': os.getenv('VERTICA_DB'),
    'autocommit': True,
}

@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 10, 1, tz='UTC'),
    catchup=True,
    tags=['final project']
)
def finproj_etl():
    pg_src = PgConnect.from_env()

    @task(task_id='load_stg_transactions')
    def task_load_transactions():
        context = get_current_context()
        execution_date = context['execution_date'] 
        StgTransactionsLoader(pg_src, VERTICA_CONFIG, log).load_transactions(execution_date.date()) 

    @task(task_id='load_stg_currencies')
    def task_load_currencies():
        StgCurrenciesLoader(pg_src, VERTICA_CONFIG, log).load_currencies()

    @task(task_id='load_dwh_global_metrics')
    def task_load_global_metrics():
        context = get_current_context()
        execution_date = context['ds']
        DWHGlobalMetricsLoader(VERTICA_CONFIG, log).load_global_metrics(execution_date)

    t1 = task_load_transactions()
    t2 = task_load_currencies()
    t3 = task_load_global_metrics()
    t1 >> t2 >> t3

finproj_etl_dag = finproj_etl()
