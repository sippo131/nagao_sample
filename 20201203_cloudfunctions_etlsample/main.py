import logging

from google.cloud import bigquery

PROJECT_ID = 'ca-nagao-test-297401'
DATASET_NAME = 'test_dataset'
DESTINATION_TABLE_NAME = '20201203_dammydata'

def sample_function_20201203_etl(event, context):
    '''
    GCS のバケットで更新があった場合に実行される関数
    '''
    # BigQuery のクライアントインスタンスを作成
    bq_client = bigquery.Client(project=PROJECT_ID)

    # Functions の引数から bucket名、ファイル名を取得
    bucket_name = "bucket-sample_20201203"
    file_name = "20201203_dammydata_table_1.csv"

    '''
    上のようにハードコードしたが、本来は動的に実装するのが拡張性が高い
    if event['attributes']['bucketId'] != None and event['attributes']['objectId'] != None:
        bucket_name = event['attributes']['bucketId']
        file_name = event['attributes']['objectId']
    else:
        bucket_name = "bucket-sample_20201203"
        file_name = "20201203_dammydata_table_1.csv"
    '''

    # load する関数を実行
    job_id_load = _gcs_to_bq(bucket_name, file_name, bq_client, DATASET_NAME, DESTINATION_TABLE_NAME)

    print(f"job_id_load : {job_id_load}")


def _gcs_to_bq(bucket_name, file_name, bq_client, dataset_name, destination_table_name):
    '''
    GCS バケットの CSV ファイルを BigQuery に load する関数
    https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv
    '''

    source_uri = 'gs://{}/{}'.format(bucket_name, file_name)
    print('import from {} to {}'.format(source_uri, destination_table_name))
    dataset_ref = bq_client.dataset(dataset_name)
    job_config = bigquery.LoadJobConfig()
    job_config.schema = [
        bigquery.SchemaField('id', 'INTEGER'),
        bigquery.SchemaField('user_id', 'INTEGER'),
        bigquery.SchemaField('event', 'STRING'),
        bigquery.SchemaField('game_id', 'INTEGER')
    ]
    # CSV ファイルをスキップする行数
    job_config.skip_leading_rows = 1
    # ファイルフォーマットは CSV
    job_config.source_format = bigquery.SourceFormat.CSV
    # （デフォルト）テーブルの末尾にデータを追加
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # load ジョブを実行開始
    load_job = bq_client.load_table_from_uri(source_uri, dataset_ref.table(destination_table_name),
                                             job_config=job_config)
    print('Starting job {}'.format(load_job.job_id))

    # load ジョブの完了を待つ
    load_job.result()
    print('Job finished.')

    # load したテーブルの行数を取得して出力
    destination_table = bq_client.get_table(dataset_ref.table(destination_table_name))
    print('Loaded {} rows.'.format(destination_table.num_rows))

    return load_job.job_id
