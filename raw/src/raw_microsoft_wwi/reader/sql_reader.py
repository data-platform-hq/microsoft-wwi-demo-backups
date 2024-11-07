from pyspark.sql import SparkSession

from raw_microsoft_wwi.configuration import IncrementalLoadConfig


class SqlReader:

    def __init__(self, app_config: IncrementalLoadConfig):
        self.config = app_config
        self.spark = SparkSession.getActiveSession()

    def read(self, query: str):
        return (
            self.spark
            .read
            .format('com.microsoft.sqlserver.jdbc.spark')
            .option('url', self.config.source_database_jdbc_url)
            .option('query', query)
            .load()
        )
