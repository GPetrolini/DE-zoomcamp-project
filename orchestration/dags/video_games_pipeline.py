import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pendulum

KAGGLE_DATASET = "gregorut/videogamesales"
DATA_DIR = "/opt/airflow/data"
BUCKET_NAME = "dtc-de-course-bucket-gustavo-2026"
PROJECT_ID = "de-zoomcamp-484312"
DATASET_ID = "video_game_data"

default_args = {
    "owner": "gustavo",
    "start_date": pendulum.datetime(2026, 3, 1, tz="America/Sao_Paulo"),
    "retries": 1,
}

with DAG(
    dag_id="video_games_sales_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["zoocamps", "kaggle", "gcp"]
) as dag:

    @task(task_id="download_dataset_kaggle")
    def download_kaggle_data():
        import kaggle
        os.makedirs(DATA_DIR, exist_ok=True)
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(
            dataset=KAGGLE_DATASET,
            path=DATA_DIR,
            unzip=True
        )
        return os.path.join(DATA_DIR, "vgsales.csv")

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="/opt/airflow/data/vgsales.csv",
        dst="raw/vgsales.csv",
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_default"
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": "vgsales_external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
                "sourceUris": [f"gs://{BUCKET_NAME}/raw/vgsales.csv"],
                "csvOptions": {"skipLeadingRows": 1},
                "autodetect": True,
            },
        },
        gcp_conn_id="google_cloud_default"
    )

    download_kaggle_data() >> upload_to_gcs >> create_external_table