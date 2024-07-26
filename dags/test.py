from airflow import DAG
from airflow.decorators import task
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.profiles import RedshiftUserPasswordProfileMapping
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import BytesIO
import os 
import pandas as pd
import vpluslib
import warnings
from modules.module import initialize
from pendulum import datetime
warnings.filterwarnings("ignore")


# Declare Variable
try:
    # Load path
    current_script_path = os.path.abspath(__file__)
    main_folder = os.path.dirname(current_script_path)

    dag_name = 'ETL_DB_Test'
    folder_config_main = 'query'
    schema_raw_redshift = 'platinum'
    folder_modeling = 'modeling'
    DBT_PROJECT_PATH = os.path.join(main_folder,folder_modeling)


    config_dir = os.path.join(main_folder,folder_config_main)
    yesterday= "where (created_at::date >= date(now()+interval '7 hour')-1) or (updated_at::date >= date(now()+interval '7 hour')-1)"

    # Load path s3
    bucket_internal = 'vtest'
    folder_s3 = 'partner_test'

    # Number Cred
    conn_num_log = 23
    conn_num_s3 = 107
    conn_num_source = 32
    conn_num_target = 45
    source_id = 'db_partner_test'
    s3_id = 's3_test'
    CONNECTION_ID = 'dwh_gold'
    SCHEMA_NAME = 'gold'
    
    # Load connection for log and S3
    conn_log = vpluslib.connect_init(conn_num_log)
    log_init = vpluslib.log_init(name_script='ETL DB Test',connection=conn_log,status_script='ETL')
    s3_1 = vpluslib.connect_init(conn_num_s3)
    
    # count row
    count_row = 0

    # Df check
    col_check = ['id','upload_date']

    #list query
    query_list = []
    list_generated_file = []
    
    for file in os.listdir(config_dir):
        name,ext = file.split('.')
        query_list.append(name)
    log_init.log('Load variable success',status_run='Success')
except Exception as eror:
    log_init.log('Load Variable failed',erors=eror,status_run='Failed')

# Connection
try:
    con_source = vpluslib.connect_init(conn_num_source)
    con_target = vpluslib.connect_init(conn_num_target)
    log_init.log('Check Connection Success',status_run='Success')
except Exception as eror:
    log_init.log('Check Connection Failed',status_run='Failed',erors=eror)

# DBT Profile
profile_config = ProfileConfig(
    profile_name="modeling",
    target_name="dev",
    profile_mapping=RedshiftUserPasswordProfileMapping(
        conn_id=CONNECTION_ID,
        profile_args={"schema": SCHEMA_NAME},
    )
)

# Load Upload Dataframe S3
def load_upload_dataframe_to_s3(query_path,format_date, key: str) -> None:
    with open(os.path.join(config_dir,query_path+'.sql'), 'r') as file:
        query = file.read().replace('\n',' ').replace('\t', '        ')
    pg_hook = PostgresHook(postgres_conn_id=source_id)
    conn = pg_hook.get_conn()
    dataframe = pd.read_sql(query.format(format_date), conn)
    if len(dataframe)>0:
        buffer = BytesIO()
        dataframe.to_csv(buffer,index=False,sep='#')
        s3_hook = S3Hook(s3_id)
        s3_hook.load_bytes(
            bytes_data=buffer.getvalue(),
            key=key,
            bucket_name=bucket_internal,
            replace=True
        )
    else:
        print(f'No Update On Dataframe {query_path}')
    return len(dataframe)

def generate_files_to_transfer(list_df_name):
    files_to_transfer = []
    for table_name in list_df_name:
        files_to_transfer.append({
            's3_key': f'partner_prov/{table_name}.csv',
            'table': table_name
        })
    return files_to_transfer

# DAGs
with DAG(start_date=datetime(2024, 7, 16),schedule=None,catchup=False,dag_id=dag_name) as dag:
    with TaskGroup('Load_Upload_Dataframe_to_s3') as load_upload_dataframe:
        for select_dataframe in query_list:
            total_row = load_upload_dataframe_to_s3(format_date=yesterday,query_path=select_dataframe,key=folder_s3+'/'+select_dataframe+'.csv')
            if total_row>0:
                list_generated_file.append(select_dataframe)
            else:
                pass
        files_to_transfer = generate_files_to_transfer(list_generated_file)

    with TaskGroup('s3_to_redshift_process') as s3_to_redshift_process:
        print(files_to_transfer)
        for file in files_to_transfer:
            s3_to_redshift = S3ToRedshiftOperator(
                task_id=f's3_to_redshift_{file["table"]}',
                schema=schema_raw_redshift,
                table=file['table'],
                s3_bucket=bucket_internal,
                s3_key=file['s3_key'],
                redshift_conn_id=CONNECTION_ID,
                aws_conn_id=s3_id,
                copy_options=[
                    "DELIMITER AS '#'",
                    "IGNOREHEADER 1"
                    ],
                method='REPLACE'
            )

    transform_data = DbtTaskGroup(
        group_id="transform_data",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
    )

    load_upload_dataframe >> s3_to_redshift_process >> transform_data

