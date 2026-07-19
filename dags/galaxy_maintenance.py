from airflow.sdk import chain, dag, Asset, Param
from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator

_DUCKDB_TABLE_URI = "duckdb://include/astronomy.db/galaxy_data"
_DUCKDB_CONN_ID = "duckdb_astronomy"

galaxy_table_asset = Asset(_DUCKDB_TABLE_URI)

@dag(
    schedule=galaxy_table_asset,
    tags=["example", "galaxies", "maintenance"],
    default_args={
        "owner": "bahabuntu",
        "retries": 2,
    },
)
def galaxy_maintenance():

    _enter_galaxy_details = HITLEntryOperator(
        task_id="enter_galaxy_details",
        subject="Please provide required information: ",
        params={
            "name": Param("", type="string"),
            "distance_from_milkyway": Param(10000, type="number"),
            "distance_from_solarsystem": Param(10000, type="number"),
            "type_of_galaxy": Param("Dwarf", type="string", enum=[
                "Dwarf Spheroidal",
                "Dwarf",
                "Irregular",
                "Spiral"
            ]),
            "characteristics": Param("", type="string")
        }
    )

    _insert_galaxy_details = SQLExecuteQueryOperator(
        task_id="insert_galaxy_details",
        conn_id=_DUCKDB_CONN_ID,
        show_return_value_in_logs=True,
        sql="""
            -- in case db was removed due to sync
            CREATE TABLE IF NOT EXISTS galaxy_data (
                name STRING PRIMARY KEY,
                distance_from_milkyway INT,
                distance_from_solarsystem INT,
                type_of_galaxy STRING,
                characteristics STRING
            );
            INSERT OR IGNORE INTO galaxy_data BY NAME
            SELECT
                $name AS name,
                $distance_from_milkyway AS distance_from_milkyway,
                $distance_from_solarsystem AS distance_from_solarsystem,
                $type_of_galaxy AS type_of_galaxy,
                $characteristics AS characteristics
        """,
        parameters={
            "name": "{{ task_instance.xcom_pull('enter_galaxy_details')['params_input']['name'] }}",
            "distance_from_milkyway": "{{ task_instance.xcom_pull('enter_galaxy_details')['params_input']['distance_from_milkyway'] }}",
            "distance_from_solarsystem": "{{ task_instance.xcom_pull('enter_galaxy_details')['params_input']['distance_from_solarsystem'] }}",
            "type_of_galaxy": "{{ task_instance.xcom_pull('enter_galaxy_details')['params_input']['type_of_galaxy'] }}",
            "characteristics": "{{ task_instance.xcom_pull('enter_galaxy_details')['params_input']['characteristics'] }}"
        }
    )

    _galaxy_dq_checks = SQLColumnCheckOperator(
        task_id="dq_checks",
        conn_id=_DUCKDB_CONN_ID,
        table="galaxy_data",
        column_mapping={
            "distance_from_milkyway": {
                "min": {"geq_to": 10000},
                "max": {"leq_to": 900000},
            },
            "distance_from_solarsystem": {
                "min": {"geq_to": 10000},
                "max": {"leq_to": 900000},
            },
        },
    )

    chain(_enter_galaxy_details, _insert_galaxy_details, _galaxy_dq_checks)

galaxy_maintenance()