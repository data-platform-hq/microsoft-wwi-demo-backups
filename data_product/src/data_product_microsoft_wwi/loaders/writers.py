import copy
import logging
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Optional, List

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

from data_product_microsoft_wwi.loaders.common import SessionInit, SingletonMeta


class WriterType(Enum):
    DELTA = "Delta"
    SQL = "SQL"
    SYNAPSE = "Synapse"
    SNOWFLAKE = "Snowflake"


class SaveType(Enum):
    OVERWRITE = 'Overwrite'
    APPEND = 'Append'
    MERGE = 'Merge'


class DataWriterAbstract(metaclass=ABCMeta):
    TYPE: str = ''

    @abstractmethod
    def write(self, df: DataFrame, table_name: Enum, save_mode: SaveType): ...


class DeltaWriter(SessionInit, DataWriterAbstract):
    __metaclass__ = SingletonMeta

    TYPE = WriterType.DELTA

    def __init__(self, catalog: str, schema: str, merge_keys_names: dict):
        self._catalog = catalog
        self._schema = schema
        self._merge_keys_names = {k.value: v for k, v in merge_keys_names.items()}
        self._database_is_created = False

    def write(self, df: DataFrame, table_name: Enum, save_mode: SaveType):
        """
        Write down the data to delta table
        """

        spark_session = self.get_spark_session()
        table_name = str(table_name.value)

        if self._database_is_created is False:
            # Run spark SQL to create DB if it does not exist
            self._database_is_created = True
            spark_session.sql(f"USE CATALOG {self._catalog}")
            spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
            spark_session.sql(f"GRANT USE SCHEMA, SELECT ON SCHEMA {self._schema} TO `account users`")

        full_table_name = f"{self._catalog}.{self._schema}.{table_name}"
        if save_mode == SaveType.OVERWRITE:
            self._overwrite(df, full_table_name)
        elif save_mode == SaveType.APPEND:
            self._append(df, full_table_name)
        elif save_mode == SaveType.MERGE:
            if not spark_session.catalog.tableExists(full_table_name):
                self._overwrite(df, full_table_name)
            else:
                merge_keys = self._merge_keys_names.get(table_name)
                self._merge(df, table_name, full_table_name, merge_keys)
        else:
            raise RuntimeError(f'Unacceptable kind of SaveType {save_mode.name}')

    def _overwrite(self, df: DataFrame, full_table_name: str):
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("overwriteSchema", "true")
         .saveAsTable(full_table_name))

    def _append(self, df: DataFrame, full_table_name: str):
        (df.write
         .format("delta")
         .mode("append")
         .saveAsTable(full_table_name))

    def _merge(self, df: DataFrame, table_name: str, full_table_name: str, merge_keys: Optional[List[Enum]]):
        """
        Merge DataFrame with current Delta table
        """
        spark_session = self.get_spark_session()
        try:
            logging.info(f'Try to get current table product for merging: {full_table_name}')
            current_delta_table = DeltaTable.forName(spark_session, full_table_name)
        except AnalysisException:
            logging.warning(
                f'Merging is impossible. Seems delta table {full_table_name} doesnt exist. try to save as new table')

            self._overwrite(df, full_table_name)  # Save table as new and return
            return

        # Exclude columns for updates
        exclude_list = copy.copy(self._merge_keys_names.get(table_name))

        (
            current_delta_table.alias("currentData")
            .merge(
                source=df.alias("newData"),
                condition=" AND ".join(list(map(lambda x: f"currentData.{x} = newData.{x}", merge_keys)))
            )
            .whenMatchedUpdate(set={column_name: f"newData.{column_name}"
                                    for column_name in list(filter(lambda x: x not in exclude_list, df.columns))})
            .whenNotMatchedInsert(
                values={column_name: f"newData.{column_name}" for column_name in df.columns})
            .execute()
        )


class SqlWriter(DataWriterAbstract):
    """
    Write down data to JDBC DB
    """
    TYPE = WriterType.SQL

    def __init__(self, db_url: str, user: str, password: str, output_table_name_paths: dict,
                 merge_df_keys_names: dict):
        self._db_url_connect = f'{db_url};user={user};password={password}'
        self._output_table_name_path = output_table_name_paths
        self._merge_keys_names = merge_df_keys_names

    def write(self, df: DataFrame, table_name: Enum, save_mode: SaveType):
        df = df.write.format("jdbc")

        df = (df
              .option("url", self._db_url_connect)
              .option("dbtable", f'{self._output_table_name_path.get(table_name)}.{table_name.value}')
              .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
              )

        if save_mode == SaveType.OVERWRITE:
            (df
             .mode("overwrite")
             .save())
        elif save_mode == SaveType.APPEND:
            (df
             .mode("append")
             .save())
        else:
            self._merge()

    def _merge(self):
        raise NotImplementedError('Merge function for SQL is not implemented yet')


class SynapseWriter(DataWriterAbstract):
    """
    Write down data to Synapse warehouse
    """
    TYPE = WriterType.SYNAPSE

    def __init__(self, db_url: str, user: str, password: str, temp_dir: str,
                 output_table_name_paths: dict, merge_df_keys_names: dict):
        self._synapse_url_connect = f'{db_url};user={user};password={password}'
        self._db_temp_dir = temp_dir
        self._output_table_name_path = output_table_name_paths
        self._merge_keys_names = merge_df_keys_names

    def write(self, df: DataFrame, table_name: Enum, save_mode: SaveType):
        df = df.write.format("com.databricks.spark.sqldw")
        if save_mode == SaveType.OVERWRITE:
            df = df.mode("overwrite")
        if save_mode == SaveType.APPEND:
            df = df.mode("append")
        (df
         .option("useAzureMSI", "true")
         .option("url", self._synapse_url_connect)
         .option("dbtable", f'{self._output_table_name_path.get(table_name)}.{table_name.value}')
         .option("tempDir", self._db_temp_dir)
         .save())


class SnowflakeWriter(DataWriterAbstract):
    """
    Write down data to Snowflake
    """
    TYPE = WriterType.SNOWFLAKE

    def __init__(self, sf_url: str, sf_user: str, sf_password: str,
                 sf_database: str, sf_schema, sf_warehouse,
                 output_table_name_paths: dict, merge_df_keys_names: dict):
        self._snowflake_url = sf_url
        self._snowflake_user = sf_user
        self._snowflake_password = sf_password
        self._snowflake_database = sf_database
        self._snowflake_schema = sf_schema
        self._snowflake_warehouse = sf_warehouse
        self._output_table_name_path = output_table_name_paths
        self._merge_keys_names = merge_df_keys_names

    def write(self, df: DataFrame, table_name: Enum, save_mode: SaveType):
        df = df.write.format("snowflake")
        if save_mode == SaveType.OVERWRITE:
            df = df.mode("overwrite")
        if save_mode == SaveType.APPEND:
            df = df.mode("append")
        (df
         .option("sfUrl", self._snowflake_url)
         .option("sfUser", self._snowflake_user)
         .option("sfPassword", self._snowflake_password)
         .option("sfDatabase", self._snowflake_database)
         .option("sfSchema", f'{self._output_table_name_path.get(table_name)}'.upper())
         .option("sfWarehouse", self._snowflake_warehouse)
         .option("dbtable", f'{table_name.value}'.upper())
         .save())


class WriterFactory:
    @staticmethod
    def get_writer(writer_type: WriterType, **kwargs) -> DataWriterAbstract:
        if writer_type == WriterType.SQL:
            return SqlWriter(db_url=kwargs['db_url'],
                             user=kwargs['user'],
                             password=kwargs['password'],
                             output_table_name_paths=kwargs['output_table_name_paths'],
                             merge_df_keys_names=kwargs['merge_keys_names'])

        elif writer_type == WriterType.DELTA:
            return DeltaWriter(catalog=kwargs['catalog'], schema=kwargs['schema'],
                               merge_keys_names=kwargs['merge_keys_names'])

        elif writer_type == WriterType.SYNAPSE:
            return SynapseWriter(db_url=kwargs['db_url'],
                                 user=kwargs['user'],
                                 password=kwargs['password'],
                                 temp_dir=kwargs['temp_dir'],
                                 output_table_name_paths=kwargs['output_table_name_paths'],
                                 merge_df_keys_names=kwargs['merge_keys_names'])

        elif writer_type == WriterType.SNOWFLAKE:
            return SnowflakeWriter(sf_url=kwargs['snowflake_url'],
                                   sf_user=kwargs['snowflake_user'],
                                   sf_password=kwargs['snowflake_password'],
                                   sf_database=kwargs['snowflake_database'],
                                   sf_schema=kwargs['snowflake_schema'],
                                   sf_warehouse=kwargs['snowflake_warehouse'],
                                   output_table_name_paths=kwargs['output_table_name_paths'],
                                   merge_df_keys_names=kwargs['merge_keys_names'])
