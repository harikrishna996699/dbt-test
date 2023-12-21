from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
from airflow import DAG
import pandas as pd



src_bucket_name = 'test-files-hsbc'
dst_bucket_name = 'stg-bucket-hari'
dst_archive_folder = 'auro_archive'
dst_error_folder = 'error'
bq_project = 'hsbc-project-408406'
bq_dataset = 'stg_dataset'

gcs_client = storage.Client()
bq_client = bigquery.Client()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'SAS_TO_BQ_DIRECT_LOAD',
    default_args=default_args,
    description='SAS TO BQ',
    schedule_interval=None, 
)

start_operator = DummyOperator(task_id='start', dag=dag)
end_operator = DummyOperator(task_id='end', dag=dag)


file_watcher = GoogleCloudStoragePrefixSensor(
    task_id='file_watcher',
    bucket=src_bucket_name,
    prefix='',
    google_cloud_conn_id='google_cloud_default',
    mode='reschedule',
    timeout=50*60,
    poke_interval=20*60,
    dag=dag
)

def move_file(src_bucket, src_blob, dst_bucket, dst_blob_name):
    src_bucket.copy_blob(src_blob, dst_bucket, dst_blob_name)
    src_blob.delete()
    print(f'File moved to {dst_bucket.name}/{dst_blob_name}')
    
src_bucket = gcs_client.get_bucket(src_bucket_name)
blobs = src_bucket.list_blobs()

def sas_to_bq():
    for blob in blobs:
        try:
            if blob.name.endswith('.sas7bdat'):
                gcs_uri = f'gs://{src_bucket_name}/{blob.name}'
                sas_data = pd.read_sas(gcs_uri, format='sas7bdat')
                sas_data = sas_data.astype(str)
                bq_table = blob.name.rsplit('.', 1)[0]
                query = f"""
                    SELECT DISTINCT table_name
                    FROM `hsbc-project-408406.stg_dataset.schema_details`
                    WHERE gcs_file_name = '{bq_table}'
                """
                query_job = bq_client.query(query)

                for row in query_job:
                    bq_table_id = row.table_name   
                table_ref = bq_client.dataset(bq_dataset).table(bq_table_id)
                job_config = bigquery.LoadJobConfig()
                job = bq_client.load_table_from_dataframe(sas_data, table_ref, job_config=job_config)
                job.result()
                print(f'Data loaded into {bq_project}.{bq_dataset}.{bq_table_id}')

                dst_blob_name = f'{dst_archive_folder}/{blob.name}'
                dst_bucket = gcs_client.get_bucket(dst_bucket_name)
                move_file(src_bucket, blob, dst_bucket, dst_blob_name)

        except Exception as e:
            print(f'Error processing file {blob.name}: {str(e)}')
            dst_blob_name = f'{dst_error_folder}/{blob.name}'
            dst_bucket = gcs_client.get_bucket(dst_bucket_name)
            move_file(src_bucket, blob, dst_bucket, dst_blob_name)
            print(f'File {blob.name} moved to error folder.')
            error_message = f'Error processing file {blob.name}: {str(e)}'
            blob=blob.name                
            insert_error_record_to_deadletter(blob, error_message)

def insert_error_record_to_deadletter(blob, error_message):
    deadletter_table_id = 'control_table' 
    deadletter_dataset_ref = bq_client.dataset(bq_dataset, project=bq_project)
    deadletter_table_ref = deadletter_dataset_ref.table(deadletter_table_id)

    current_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    row_to_insert = {
        'projectName': bq_project,'FileName': blob,'errorMsg': f'{error_message} in file {blob}','ingestionTimestamp': current_ts
    }

    errors = bq_client.insert_rows_json(deadletter_table_ref, [row_to_insert])

    if errors == []:
        print('Error record inserted into deadletter table successfully.')
    else:
        print(f'Error occurred while inserting error record into deadletter table: {errors}')

sas_to_bq_task = PythonOperator(
    task_id='convert_sas_to_csv',
    python_callable=sas_to_bq,
    dag=dag,
)

file_watcher >> start_operator >> sas_to_bq_task >> end_operator