import pandas as pd
from google.cloud import bigquery
from google.cloud import storage



src_bucket_name = 'test-files-hsbc'
dst_bucket_name = 'stg-bucket-hari'
dst_archive_folder = 'auro_archive'
dst_error_folder = 'error'
bq_project = 'hsbc-project-408406'
bq_dataset = 'stg_dataset'

gcs_client = storage.Client()
bq_client = bigquery.Client()

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
