from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from google.cloud import storage, bigquery
from datetime import datetime, timedelta
from airflow import DAG
import os

source_bucket_name = "test-files-hsbc"
destination_bucket_name = 'stg-bucket-hari'
gcs_stg_path = 'stg_avro_files'  
gcs_archive_path = 'avro_archive'
bq_project_id = 'hsbc-project-408406'
bq_dataset_id = 'stg_dataset'

storage_client = storage.Client()
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
    'AVRO_TO_BQ_FINAL',
    default_args=default_args,
    description='AVRO TO BQ',
    schedule_interval=None, 
)

start_operator = DummyOperator(task_id='start', dag=dag)
end_operator = DummyOperator(task_id='end', dag=dag)

file_watcher = GoogleCloudStoragePrefixSensor(
    task_id='file_watcher',
    bucket=source_bucket_name,
    prefix='',
    google_cloud_conn_id='google_cloud_default',
    mode='reschedule',
    timeout=50*60,
    poke_interval=20*60,
    dag=dag
)

def gcs_to_bq_avro():
    gcs_bucket = storage_client.get_bucket(source_bucket_name)
    blobs = gcs_bucket.list_blobs()

    for blob in blobs:
        if blob.name.endswith('.avro'):
            try:
                bq_table = blob.name.rsplit('.', 1)[0]
                query = f"""
                    SELECT DISTINCT table_name
                    FROM `hsbc-project-408406.stg_dataset.schema_details`
                    WHERE gcs_file_name = '{bq_table}'
                """
                query_job = bq_client.query(query)

                for row in query_job:
                    bq_table_id = row.table_name
                    dataset_ref = bq_client.dataset(bq_dataset_id, project=bq_project_id)
                    table_ref = dataset_ref.table(bq_table_id)

                    job_config = bigquery.LoadJobConfig(
                        source_format=bigquery.SourceFormat.AVRO,
                    )

                    with gcs_bucket.blob(blob.name).open('rb') as source_file:
                        load_job = bq_client.load_table_from_file(
                            source_file,
                            table_ref,
                            location='europe-west1',
                            job_config=job_config,
                        )

                    load_job.result()

                    print(f'File {blob.name} loaded into BigQuery table.')

                    archive_blob_name = f'{gcs_archive_path}/{os.path.basename(blob.name)}'
                    archive_destination_blob = f'{gcs_archive_path}/{os.path.basename(blob.name)}'
                    gcs_bucket.copy_blob(blob, storage_client.bucket(destination_bucket_name), archive_destination_blob)
                    blob.delete()

                    print(f'File {blob.name} moved to {gcs_archive_path} in {destination_bucket_name}.')

            except Exception as e:
                error_blob_name = f'error/{os.path.basename(blob.name)}'
                error_destination_blob = f'error/{os.path.basename(blob.name)}'
                gcs_bucket.copy_blob(blob, storage_client.bucket(destination_bucket_name), error_destination_blob)
                blob.delete()
                print(f'File {blob.name} moved to error folder in {destination_bucket_name}.')
                error_message = f'Error loading file into BigQuery: {str(e)}'
                blob = blob.name
                insert_error_record_to_deadletter(blob, error_message)

    print('All files processed.')

def insert_error_record_to_deadletter(blob, error_message):
    deadletter_table_id = 'control_table' 
    deadletter_dataset_ref = bq_client.dataset(bq_dataset_id, project=bq_project_id)
    deadletter_table_ref = deadletter_dataset_ref.table(deadletter_table_id)

    current_ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    row_to_insert = {
        'projectName': bq_project_id,
        'FileName': blob,
        'errorMsg': f'{error_message} in file {blob}',
        'ingestionTimestamp': current_ts
    }

    errors = bq_client.insert_rows_json(deadletter_table_ref, [row_to_insert])

    if errors == []:
        print('Error record inserted into deadletter table successfully.')
    else:
        print(f'Error occurred while inserting error record into deadletter table: {errors}')

execute_code_operator_avro = PythonOperator(
    task_id='gcs_to_bq_avro',
    python_callable=gcs_to_bq_avro,
    dag=dag,
)

file_watcher >> start_operator >> execute_code_operator_avro >> end_operator