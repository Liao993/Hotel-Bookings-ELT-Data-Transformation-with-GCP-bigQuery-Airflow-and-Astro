from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator # type: ignore
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from include.soda.check_function import check # type: ignore
#from include.soda.check_function import check

from astro import sql as aql # type: ignore
from astro.files import File # type: ignore
from astro.sql.table import Table, Metadata #type: ignore
from astro.constants import FileType # type: ignore

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG # type: ignore
from cosmos.airflow.task_group import DbtTaskGroup # type: ignore
from cosmos.constants import LoadMode # type: ignore
from cosmos.config import RenderConfig # type: ignore

@dag(
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=['bookings'],
)

def bookings():
  #load data to cloud storage in GCP
  upload_csv_to_gcs = LocalFilesystemToGCSOperator(
    task_id='upload_csv_to_gcs',
    src='/usr/local/airflow/include/dataset/hotel_bookings.csv',
    dst='raw/hotel_bookings.csv',
    bucket='hotel-bookings-cs',
    gcp_conn_id='gcp',
    mime_type='text/csv',
  )
  #create schema into bigQuery
  create_bookings_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bookings_dataset',
    dataset_id='bookings',
    gcp_conn_id='gcp',
  )
  #load data from gcs to bigQuery
  gcs_to_raw = aql.load_file(
    task_id='gcs_to_raw',
    input_file=File(
      'gs://hotel-bookings-cs/raw/hotel_bookings.csv',
      conn_id='gcp',
      filetype=FileType.CSV,
    ),
    output_table=Table(
      name='raw_bookings',
      conn_id='gcp',
      metadata=Metadata(schema='bookings')
    ),
    use_native_support=False,
  )

  ## Connect to soda
  soda_test_connection = PythonOperator(
        task_id="soda_test_connection",
        python_callable=check,
        op_kwargs={
            "scan_name": "test_connection",
            "checks_subpath": None,  # No checks for connection test
            "data_source": "bookings",
            "config_file": "/usr/local/airflow/include/soda/configuration.yml",
            "test_connection": False,  # Flag to test connection
        },
    )
  # Run soda quality check for raw bookings data
  soda_check = PythonOperator(
        task_id="soda_quality_check",
        python_callable=check,
        op_kwargs={
            "scan_name": "check_load",
            "checks_subpath": "sources/raw_bookings_check.yml",
            "data_source": "bookings",
            "config_file": "/usr/local/airflow/include/soda/configuration.yml",
        },
    )

  # Add dbt transformation task group
  transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )
  
  # Run Soda quality check for transformed data
  soda_check_transform = PythonOperator(
        task_id="soda_quality_check_transform",
        python_callable=check,
        op_kwargs={
            "scan_name": "check_transform",
            "checks_subpath": "transform",
            "data_source": "bookings",
            "config_file": "/usr/local/airflow/include/soda/configuration.yml",
        },
    )

 

  upload_csv_to_gcs >> create_bookings_dataset >> gcs_to_raw >> soda_test_connection >> soda_check >> transform >> soda_check_transform


  
bookings()