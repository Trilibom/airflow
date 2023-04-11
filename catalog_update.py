"""
Обновление справочников из Catalog

Справочники копируются в буферные таблицы в схеме buffer, а затем в рамках одной
транзакции происходит перевалка данных в таблицы справочников в схеме raw. Так
сделано для исключения случаев пустых словарей.


CONNECTIONS
 - postgres_catalog:     база источник
 - greenplum:            промежуточная база


VARIABLES
 - catalog_tables:       список таблиц из Catalog через запятую
 - mailto:               список почтовых ящиков для рассылки алертов

"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.transfers.operators.stream_transfer import StreamTransfer
from airflow.exceptions import AirflowSkipException
from airflow.utils.email import send_email
import logging

# from utils.priority import default_priority  # Temporary comment out


with DAG(
    dag_id="catalog_update",
    tags=["catalog"],
    #default_args=default_priority,     # Temporary comment out
    start_date=days_ago(1),
    schedule_interval='10 22 * * *',    # каждый день в 1:10 MSK
    max_active_runs=1,
) as dag:

    default_var=default_catalog_tables = "container, container_type, country, currency, filial, etsng, gng, location, organization, railadm, railstation, railway, region, service, service_details, shipment_status, shipment_status_details, stations, wagon, wagon_type, user_info"

    mailto = Variable.get("mailto", default_var="")
    tables = Variable.get("catalog_tables", default_var=default_catalog_tables)
    tables = [x.strip() for x in tables.split(",")]
    engine = Variable.get("catalog_tables_engine", default_var="")

    conn = BaseHook.get_connection(conn_id="greenplum")

    dbt = BashOperator(
        task_id="dbt_run",
        # pool="dbt",
        bash_command=f"dbt run -m"
            " catalog_country stage_country hub_country sat_country countries"
            " catalog_rail_administration stage_rail_administration hub_rail_administration sat_rail_administration link_rail_administration_country rail_administrations"
            " catalog_railway stage_railway hub_railway sat_railway link_rail_administration_railway railways"
            " catalog_region stage_region hub_region sat_region link_region_country regions"
            " catalog_rail_station stage_rail_station hub_rail_station sat_rail_station link_rail_station_country link_rail_station_region link_rail_station_railway rail_stations",
        env={
            "DBT_HOST": conn.host,
            "DBT_PORT": f"{conn.port}",
            "DBT_DBNAME": conn.schema,
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_SSL": "require", #"disable",
            "DBT_SCHEMA": "public",
            "DBT_USE_COLORS": "0",
        },
        cwd="dbt/project"
    )

    for table in tables:
        create_buffer = PostgresOperator(
            task_id=f"create_buffer_{table}",
            postgres_conn_id="greenplum",
            sql=f"sql/create/{table}.sql",
            params={
                "schema": "buffer",
                "engine": engine,
            }
        )

        create_table = PostgresOperator(
            task_id=f"create_table_{table}",
            postgres_conn_id="greenplum",
            sql=f"sql/create/{table}.sql",
            params={
                "schema": "raw",
                "engine": engine,
            }
        )

        @task(task_id=f"if_not_empty_source_{table}")
        def if_not_empty_source(table):
            if table in ['filial', 'stations']:
                logging.info(f"Skip checking table `{table}` because it contains joined query.")
                return

            hook = PostgresHook(postgres_conn_id="postgres_catalog")
            count = hook.get_first(f"SELECT count(*) FROM {table}")[0]
            if count == 0:
                logging.error(f'Catalog table `{table}` is empty in source!')
                send_email(
                    to=mailto,
                    subject='Airflow: Catalog table is empty in source!',
                    html_content=f"<h3>Empty table detected!</h3>Catalog table `{table}` is empty in source! Skiping...",
                )
                raise AirflowSkipException

        truncate_buffer = PostgresOperator(
            task_id=f"truncate_buffer_{table}",
            postgres_conn_id="greenplum",
            sql=f"TRUNCATE TABLE buffer.{table}",
        )

        transfer = StreamTransfer(
            task_id=f"transfer_{table}",
            source_conn_id="postgres_catalog",
            destination_conn_id="greenplum",
            destination_table=f"buffer.{table}",
            sql=f"sql/select/{table}.sql",
            retries=5,
        )

        replace = PostgresOperator(
            task_id=f"replace_{table}",
            postgres_conn_id="greenplum",
            sql=f"""
                BEGIN;
                TRUNCATE TABLE raw.{table};
                INSERT INTO raw.{table}
                SELECT * FROM buffer.{table};
                COMMIT;
            """,
        )

        create_buffer >> \
            create_table >> \
            if_not_empty_source(table) >> \
            truncate_buffer >> \
            transfer >> \
            replace >> \
            dbt
