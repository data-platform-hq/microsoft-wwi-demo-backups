from pyspark.sql import SparkSession
from delta import DeltaTable

from raw_microsoft_wwi.configuration import Config


class Writer:
    def __init__(self, config: Config):
        self.config = config
        self.first_run = True

    def write_table(self, table_data, table_name):
        spark = SparkSession.getActiveSession()

        if self.first_run:
            self.first_run = False
            spark.sql(f"USE CATALOG {self.config.catalog}")
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}")
            spark.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {self.config.schema} TO `account users`")
        (
            table_data.write.format('delta').mode('overwrite')
            .option("overwriteSchema", "true")
            .saveAsTable(f'{self.get_full_table_name(table_name)}')
        )

    def overwrite_delta_table(self, table_data, table_name):
        (
            table_data.write.format('delta')
            .mode('overwrite')
            .save(self.get_delta_table_path(table_name))
        )

    def upsert_delta_regular_table(self, updated_data, table_name):
        table_data = self.get_delta_table_using_full_name(table_name)
        join_column_name = updated_data.columns[0]
        (
            table_data.alias("oldData")
            .merge(updated_data.alias("newData"), f"oldData.{join_column_name} = newData.{join_column_name}")
            .whenMatchedUpdate(set={column_name: f"newData.{column_name}" for column_name in updated_data.columns})
            .whenNotMatchedInsert(values={column_name: f"newData.{column_name}" for column_name in updated_data.columns})
            .execute()
        )

    def upsert_delta_system_versioned_table(self, updated_data, table_name):
        table_data = self.get_delta_table_using_full_name(table_name)
        merge_condition = (
            f"oldData.{updated_data.columns[0]} = newData.{updated_data.columns[0]} AND "
            f"oldData.ValidFrom = newData.ValidFrom"
        )
        fields_dict_update = {column_name: f"newData.{column_name}" for column_name in updated_data.columns[:-1]}
        fields_dict_insert = {column_name: f"newData.{column_name}" for column_name in updated_data.columns}
        (
            table_data.alias("oldData")
            .merge(updated_data.alias("newData"), merge_condition)
            .whenMatchedUpdate(set=fields_dict_update)
            .whenNotMatchedInsert(values=fields_dict_insert)
            .execute()
        )

    def upsert_delta_table_debezium(self, updated_data, table_name):
        table_data = self.get_delta_table_using_path(table_name)
        join_column_name = updated_data.columns[0]
        (
            table_data.alias("oldData")
            .merge(updated_data.alias("newData"), f"oldData.{join_column_name} = newData.{join_column_name}")
            .whenMatchedUpdate(set={column_name: f"newData.{column_name}" for column_name in updated_data.columns})
            .whenNotMatchedInsert(
                values={column_name: f"newData.{column_name}" for column_name in updated_data.columns})
            .execute()
        )

    def get_full_table_name(self, table_name):
        return f'{self.config.catalog}.{self.config.schema}.{table_name}'

    def get_delta_table_path(self, table_name: str) -> str:
        return f'{self.config.input_delta_tables_path}/{table_name.replace("_", "/")}'

    def get_delta_table_using_full_name(self, table_name: str) -> DeltaTable:
        spark = SparkSession.getActiveSession()
        return DeltaTable.forName(spark, self.get_full_table_name(table_name))

    def get_delta_table_using_path(self, table_name: str) -> DeltaTable:
        spark = SparkSession.getActiveSession()
        return DeltaTable.forPath(spark, self.get_delta_table_path(table_name))
