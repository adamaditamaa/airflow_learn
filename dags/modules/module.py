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
from pendulum import datetime
warnings.filterwarnings("ignore")

class initialize():
    def __init__(self,s3_id,source_id):
        self.source_id = source_id
        self.s3_id = s3_id

    # Load Upload Dataframe S3
    def load_upload_dataframe_to_s3(self, query_path,format_date,query_dir,source_id,s3_id, key: str, bucket_internal) -> None:
        with open(os.path.join(query_dir,query_path+'.sql'), 'r') as file:
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

    def generate_files_to_transfer(self, list_df_name, folder_s3):
        files_to_transfer = []
        for table_name in list_df_name:
            files_to_transfer.append({
                's3_key': f'{folder_s3}/{table_name}.csv',
                'table': table_name
            })
        return files_to_transfer