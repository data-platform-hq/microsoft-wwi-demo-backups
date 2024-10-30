import logging
from datetime import datetime
from enum import Enum

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, desc
from pyspark.sql.utils import AnalysisException

DEFAULT_SINCE_DATETIME = datetime(year=1900, month=1, day=1)


class DataFilter:
    class LoadDatetimeKeyError(Exception):
        pass

    @staticmethod
    def filter(since: datetime, to: datetime, data_frame: DataFrame) -> DataFrame:
        try:
            return data_frame.where((data_frame.LoadDatetime > since) & (data_frame.LoadDatetime <= to))

        except AttributeError as e:
            raise DataFilter.LoadDatetimeKeyError(str(e))

    @staticmethod
    def _get_last_value_by_key(data_frame_check: DataFrame, col_name: str):
        try:
            return data_frame_check.sort(desc(col_name)).select(col(col_name)).first()[0]
    
        except TypeError:
            return 0

    def get_offset(self, reader, table: Enum, output_transformed_tables_primary_keys_names: dict) -> int:
        try:
            offset = self._get_last_value_by_key(
                reader.get_transformed_df(table),
                str(output_transformed_tables_primary_keys_names.get(table))
            ) + 1
        except AnalysisException:
            offset = 0

        return offset

    def get_since_date(self, reader, table: Enum, key_name: str) -> datetime:
        try:
            # Get last value for LoadDatetime from product fact Sale
            date_time_since = self._get_last_value_by_key(reader.get_transformed_df(table), key_name)

            if date_time_since is None:
                date_time_since = DEFAULT_SINCE_DATETIME

            logging.info(f'Found last time loading to product tables: {date_time_since}')

        except (AnalysisException, TypeError) as e:
            logging.warning(
                f'It seems the table {table.value} does not exist or empty. Got error {str(e)}. '
                f'So set a since time as 1900 year')
            date_time_since = DEFAULT_SINCE_DATETIME

        return date_time_since
