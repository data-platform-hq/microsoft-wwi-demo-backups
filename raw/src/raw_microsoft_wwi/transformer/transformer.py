from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


class Transformer:
    @classmethod
    def get_ingestion_last(cls, df: DataFrame):
        if df.count() == 0:
            return None
        ingestion_last = df.select("LoadDatetime").orderBy(df.LoadDatetime.desc()).limit(1)
        ingestion_last = ingestion_last.collect()[0][0]
        return ingestion_last

    @classmethod
    def append_query_filter_regular_tables(cls, existing_df: DataFrame, query: str, ingestion_new: datetime) -> str:
        ingestion_last = cls.get_ingestion_last(existing_df)

        if ingestion_last is None:  # Proceed with full load by ingestion_new datetime
            transformed_query = f"{query} WHERE LastEditedWhen <= '{ingestion_new}'"
        else:
            transformed_query = (
                f"{query} WHERE LastEditedWhen <= '{ingestion_new}' "
                f"AND LastEditedWhen > '{ingestion_last}'"
            )
        return transformed_query

    @classmethod
    def append_filter_by_valid_from_valid_to(cls, existing_df: DataFrame, query: str, ingestion_new: datetime) -> str:
        ingestion_last = cls.get_ingestion_last(existing_df)
        union = "\nUNION\n"
        main_table_query, archive_table_query = query.split(union)

        if ingestion_last is None:  # Proceed with full load by ingestion_new datetime
            transformed_query = (
                f"{main_table_query} WHERE\n"
                f"(ValidFrom <= '{ingestion_new}')"
                f"{union} ALL\n"
                f"{archive_table_query} WHERE\n"
                f"(ValidTo <= '{ingestion_new}')"
                f"\nOR\n"
                f"(ValidFrom <= '{ingestion_new}')"
            )
        else:
            transformed_query = (
                f"{main_table_query} WHERE\n"
                f"( ValidFrom > '{ingestion_last}' AND ValidFrom <= '{ingestion_new}')"
                f"{union}"
                f"{archive_table_query} WHERE\n"
                f"( ValidTo > '{ingestion_last}' AND ValidTo <= '{ingestion_new}')"
                f"\nOR\n"
                f"( ValidFrom > '{ingestion_last}' AND ValidFrom <= '{ingestion_new}')"
            )
        return transformed_query

    @classmethod
    def transform_table(cls, table: DataFrame, load_datetime: datetime):
        return table.withColumn("LoadDatetime", lit(load_datetime))
