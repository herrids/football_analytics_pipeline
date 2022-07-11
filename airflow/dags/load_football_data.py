from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2014, 8, 24),
    'end_date': datetime(2021, 6, 30),
    'catchup': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=300),

}

dag = DAG('load_football_data',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@daily'
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql='create_tables.sql',
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_key="events/events_{ds}",
    s3_bucket="capstone-football",
    table="staging_events",
)

stage_attributes_to_redshift = StageToRedshiftOperator(
    task_id='Stage_attributes',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_key="attribute_events/attribute_events_{ds}",
    s3_bucket="capstone-football",
    table="fact_event_attributes",
    append=True
)

stage_matches_to_redshift = StageToRedshiftOperator(
    task_id='Stage_matches',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_key="matches",
    s3_bucket="capstone-football",
    table="staging_matches",
    append=False
)

stage_players_to_redshift = StageToRedshiftOperator(
    task_id='Stage_players',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",
    s3_key="players",
    s3_bucket="capstone-football",
    table="staging_players",
    append=False
)

load_players_table = LoadDimensionOperator(
    task_id='Load_players_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_players",
    query=SqlQueries.player_table_insert,
    append=False
)

load_events_table = LoadFactOperator(
    task_id='Load_events_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="fact_events",
    query=SqlQueries.event_table_insert
)

load_matches_table = LoadDimensionOperator(
    task_id='Load_matches_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_matches",
    query=SqlQueries.match_table_insert,
    append=False
)

load_teams_table = LoadDimensionOperator(
    task_id='Load_teams_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_teams",
    query=SqlQueries.team_table_insert,
    append=False
)

load_types_table = LoadDimensionOperator(
    task_id='Load_types_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_types",
    query=SqlQueries.type_table_insert,
    append=False
)

load_attributes_table = LoadDimensionOperator(
    task_id='Load_attributes_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="dim_type_attributes",
    query=SqlQueries.attribute_table_insert,
    append=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_table_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['fact_events', "fact_event_attributes", 'dim_matches', 'dim_teams',
            'dim_types', 'dim_type_attributes', "dim_players"],
    queries=[SqlQueries.check_empty_tables]
)

check_event_duplicates = DataQualityOperator(
    task_id='event_duplicates_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['fact_events'],
    columns='event_id',
    queries=[SqlQueries.check_duplicate_rows]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

staging_tasks = [
    stage_events_to_redshift,
    stage_matches_to_redshift,
    stage_players_to_redshift,
    stage_attributes_to_redshift]

data_quality_tasks = [run_quality_checks, check_event_duplicates]

start_operator >> create_tables_task
create_tables_task >> stage_events_to_redshift
create_tables_task >> stage_players_to_redshift
stage_events_to_redshift >> [
    stage_matches_to_redshift, 
    stage_attributes_to_redshift, 
    stage_players_to_redshift]
stage_players_to_redshift >> load_players_table
load_teams_table >> load_players_table
load_players_table >> load_events_table
stage_attributes_to_redshift >> [load_attributes_table, load_types_table]
stage_matches_to_redshift >> [load_matches_table,load_teams_table]
load_matches_table >> load_events_table
load_players_table >> data_quality_tasks
load_events_table >> data_quality_tasks
load_types_table >> data_quality_tasks
load_attributes_table >> data_quality_tasks
load_matches_table >> data_quality_tasks
load_teams_table >> data_quality_tasks
run_quality_checks >> end_operator
check_event_duplicates >> end_operator
