from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import NewType, Union, Optional

from pyspark.sql import DataFrame

from data_product_microsoft_wwi.configuration import InputTableNames, input_table_name_path, OutputDimensionsTableNames, \
    output_table_name_path, OutputFactTableName
from data_product_microsoft_wwi.loaders.common import SessionInit, SingletonMetaBasic

TablePath = NewType('TablePath', Union[InputTableNames, OutputDimensionsTableNames, OutputFactTableName])


class DataReaderAbstract(metaclass=ABCMeta):
    """
    Read and hold DataFrame
    """

    def __init__(self):
        self._data_frames = {}

    def data_frame_exists(self, table_name: TablePath) -> bool:
        return table_name in self._data_frames

    def set_data_frame(self, table_name: TablePath, data_frame: DataFrame):
        """
        Set custom data frame to a dataframe warehouse
        """
        self._data_frames[table_name] = data_frame

    def clear_data_frame_storage(self, df_name: Optional[Enum] = None):
        if df_name:
            del self._data_frames[df_name]
        else:
            self._data_frames = {}

    @staticmethod
    def _get_table_path(table_name: TablePath) -> str:
        if isinstance(table_name, InputTableNames):
            return input_table_name_path.get(table_name)
        elif isinstance(table_name, OutputDimensionsTableNames) or isinstance(table_name, OutputFactTableName):
            return output_table_name_path.get(table_name)
        else:
            RuntimeError('`table_name` should be instance of `InputTableNames` or `OutputTableNames`')

    @abstractmethod
    def get_source_df(self, table_name: TablePath):
        ...


class CombineMetaClasses(type(SingletonMetaBasic), type(DataReaderAbstract)):
    """
    It's a trick to combine multiple metaclass inheritance
    """
    pass


class DeltaReader(SessionInit, DataReaderAbstract, metaclass=CombineMetaClasses):
    """
    Read and hold DataFrame
    """

    def __init__(self, source_catalog: str, source_schema: str,
                 target_catalog: str | None = None, target_schema: str | None = None):
        DataReaderAbstract.__init__(self)
        self._data_frames = {}
        self._source_catalog = source_catalog
        self._source_schema = source_schema
        self._target_catalog = target_catalog
        self._target_schema = target_schema

    def get_table_from_delta(self, full_table_name: str):
        return self.get_spark_session().read.format("delta").table(full_table_name)

    def get_transformed_df(self, table_name: TablePath) -> DataFrame:
        if self._data_frames.get(table_name):
            return self._data_frames.get(table_name)
        elif self._target_catalog and self._target_schema:
            self._data_frames[table_name] = \
                self.get_table_from_delta(f"{self._target_catalog}.{self._target_schema}.{str(table_name.value)}")
            return self._data_frames[table_name]
        else:
            raise ValueError(f"Values target_catalog={self._target_catalog} and target_schema={self._target_schema} "
                             f"are not set for this type of transformer.")

    def get_source_df(self, table_name: TablePath) -> DataFrame:
        if self._data_frames.get(table_name):
            return self._data_frames.get(table_name)
        else:
            self._data_frames[table_name] = \
                self.get_table_from_delta(f"{self._source_catalog}.{self._source_schema}.{str(table_name.value)}")
            return self._data_frames[table_name]
