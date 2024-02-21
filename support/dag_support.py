import os
import logging
from pyspark.sql import SparkSession
from delta import *
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

from support.utils import GetExcelData, GetParquetData
from support.schemas import sales_schema, chn_group_schema

# * Bronze layer:


def extract_source_data(
    path: str = "/opt/airflow/source_data",
) -> list:
    files = [os.path.join(path, x) for x in os.listdir(path)]

    return files


def load_to_gcs_data(ti: object) -> None:
    from google.cloud import storage
    from google.oauth2 import service_account

    files: list = ti.xcom_pull(task_ids="extract_source_data")

    try:
        bucket_name = "poc_bucket_gcp"
        credentials = service_account.Credentials.from_service_account_file(
            "/opt/airflow//support/key.json"
        )
        client = storage.Client(credentials=credentials)
        bucket = client.get_bucket(bucket_name)

        for file in files:
            logging.debug(f"File: {file} started the upload session")  #! Debug line
            dest_path = os.path.join("Bronze", file_name)
            file_name = os.path.basename(file)

            blob = bucket.blob(dest_path)
            blob.upload_from_filename(file)
            logging.debug(f"File: {file} finished the upload session")  #! Debug line

        logging.info("Data files uploaded sucessfully")
        return "load_sucess"

    except Exception as error:
        logging.error(error)
        return "load_fail"


# ---

# * Silver layer:


def pull_gcs_data():
    gcp_client = GetExcelData("poc_bucket_gcp")
    gcp_client.get_data("Bronze", "/opt/airflow/tmp/to_process")
    return


def transform_to_df():
    builder = (
        SparkSession.builder.appName("Inbev Case ETL")
        .config("spark.python.profile.memory", True)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    files = [
        os.path.basename(x) for x in extract_source_data("/opt/airflow/tmp/to_process")
    ]

    # pandas_df_sales = pd.read_excel(str([x for x in files if "sales" in x][0]))

    # pandas_df_chn_group = pd.read_excel(str([x for x in files if "channel" in x][0]))

    df_sales = spark.createDataFrame(
        pd.read_excel(str([x for x in files if "sales" in x][0])), schema=sales_schema
    )

    df_chn_group = spark.createDataFrame(
        pd.read_excel(str([x for x in files if "channel" in x][0])),
        schema=chn_group_schema,
    )

    df_sales.show(5)

    df_chn_group.show(5)

    return


# ---

# * Gold layer:
# ---
