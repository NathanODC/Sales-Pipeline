from __future__ import annotations
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession


class GetBucketData(ABC):
    def __init__(self, client: object) -> None:
        self.client = client
        self.spark = (
            SparkSession.builder.appName("Winair data process")
            .config("spark.python.profile.memory", True)
            .config("spark.executor.memory", "2g")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        ).getOrCreate()

    @abstractmethod
    def get_data(self, path: str, mode: str):
        pass

    @abstractmethod
    def data_to_dataframe(self):
        pass


class GetExcelData(GetBucketData):
    def __init__(
        self,
        bucket_name: str,
    ) -> None:
        super.__init__()
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(bucket_name)

    def get_data(self, path: str, dest_path: str, mode: str = "r") -> None:
        blobs = self.bucket.list_blobs(prefix=path)
        for blob in blobs:
            blob.download_to_filename(dest_path)
        return

    def data_to_dataframe(
        self,
        basename_files: list,
        sales_schema: object,
        chn_group_schema: object,
        temp_folder: str = "./tmp",
    ) -> object:
        # pandas_df_sales = pd.read_excel(str([x for x in files if "sales" in x][0]))

        # pandas_df_chn_group = pd.read_excel(str([x for x in files if "channel" in x][0]))

        df_sales = spark.createDataFrame(
            pd.read_excel(str([x for x in basename_files if "sales" in x][0])),
            schema=sales_schema,
        )

        df_chn_group = spark.createDataFrame(
            pd.read_excel(str([x for x in basename_files if "channel" in x][0])),
            schema=chn_group_schema,
        )

        return


class GetParquetData(GetBucketData):
    def __init__(self, bucket_name: str) -> None:
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(bucket_name)

    def get_data(self, path: str, dest_path: str, mode: str = "r") -> None:
        blobs = self.bucket.list_blobs(prefix=path)
        for blob in blobs:
            blob.download_to_filename(dest_path)
        return

    def data_to_dataframe(self, temp_folder: str = "./tmp") -> object:
        return
