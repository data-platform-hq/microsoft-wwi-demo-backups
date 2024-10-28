from pyspark.sql import SparkSession

from raw_microsoft_wwi.configuration import Config


class DeltaReader:

    def __init__(self, app_config: Config):
        self.config = app_config
        self.spark = SparkSession.getActiveSession()

    def read(self, table_name: str):
        return (
            self.spark
            .read
            .format('delta')
            .table(f"{self.config.catalog}.{self.config.schema}.{table_name}")
        )
