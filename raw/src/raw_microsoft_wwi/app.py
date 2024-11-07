import datetime
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from raw_microsoft_wwi.configuration import Config
from raw_microsoft_wwi.configuration.configuration import LaunchModes
from raw_microsoft_wwi.consumer import ConsumersFabric
from raw_microsoft_wwi.reader import SqlReader
from raw_microsoft_wwi.reader.delta_reader import DeltaReader
from raw_microsoft_wwi.transformer.transformer import Transformer
from raw_microsoft_wwi.writer import Writer


def get_schemas_path() -> str:
    return os.path.join(os.path.dirname(__file__), "schemas")


def get_queries_path() -> str:
    return os.path.join(os.path.dirname(__file__), "queries")


def create_table_if_not_exists(config, table_name, table_type, writer):
    spark = SparkSession.getActiveSession()
    full_table_name = f"{config.catalog}.{config.schema}.{table_name}"
    if not spark.catalog.tableExists(full_table_name):
        create_table(table_name, table_type, writer)


def create_table(table_name, table_type, writer):
    spark = SparkSession.getActiveSession()
    schemas_path = get_schemas_path()
    schema_file = open(
        os.path.join(
            schemas_path,
            f"{table_type}_tables_schemas",
            f"{table_name}.json"
        ), 'r'
    )
    schema = StructType.fromJson(json.loads(schema_file.read()))
    empty_df = spark.createDataFrame([], schema)
    writer.write_table(empty_df, table_name)


def get_table(config, table_name, table_type, writer, delta_reader):
    create_table_if_not_exists(config, table_name, table_type, writer)
    return delta_reader.read(table_name)


def load_regular_tables(config, delta_reader, sql_reader, writer):

    load_datetime = os.getenv("INGESTION_DATETIME", datetime.datetime.now())

    for table_name in config.regular_tables:
        query_file = open(
            os.path.join(
                get_queries_path(),
                "regular_tables_queries",
                f"{table_name}.sql"
            ), 'r'
        )

        existing_df = get_table(
            config,
            table_name,
            table_type='regular',
            writer=writer,
            delta_reader=delta_reader
        )

        transformed_query = Transformer.append_query_filter_regular_tables(
            existing_df, query_file.read(), load_datetime
        )

        delta_table = sql_reader.read(transformed_query)
        transformed_table = Transformer.transform_table(delta_table, load_datetime)
        writer.upsert_delta_regular_table(transformed_table, table_name)


def load_system_versioned_tables(config, delta_reader, sql_reader, writer):

    load_datetime = os.getenv("INGESTION_DATETIME", datetime.datetime.now())

    for table_name in config.system_versioned_tables:
        query_file = open(
            os.path.join(
                get_queries_path(),
                "system_versioned_tables_queries",
                f"{table_name}.sql"),
            'r'
        )

        existing_df = get_table(
            config,
            table_name,
            table_type='system_versioned',
            writer=writer,
            delta_reader=delta_reader
        )

        transformed_query = Transformer.append_filter_by_valid_from_valid_to(
            existing_df, query_file.read(), load_datetime
        )

        delta_table = sql_reader.read(transformed_query)
        transformed_table = Transformer.transform_table(delta_table, load_datetime)
        writer.upsert_delta_system_versioned_table(transformed_table, table_name)


def load_debezium_tables(config, writer):
    schemas_path = get_schemas_path()
    ConsumersFabric.get_consumer('', '', config, schemas_path, writer, schema_generation=True).generate_schemas()
    consumers = []
    for table_name in config.system_versioned_tables:
        consumers.append(ConsumersFabric.get_consumer(table_name, 'system_versioned', config, schemas_path, writer))
        create_table_if_not_exists(config, table_name, 'system_versioned', writer)
    for table_name in config.regular_tables:
        consumers.append(ConsumersFabric.get_consumer(table_name, 'regular', config, schemas_path, writer))
        create_table_if_not_exists(config, table_name, 'regular', writer)
    for consumer in consumers:
        consumer.consume()
    spark = SparkSession.getActiveSession()
    spark.streams.awaitAnyTermination()


def main(sql_reader: SqlReader, delta_reader: DeltaReader, writer: Writer,
         load_datetime: datetime.datetime, config: Config):

    if config.mode == LaunchModes.INCREMENTAL:
        os.environ["INGESTION_DATETIME"] = str(load_datetime)
        load_regular_tables(config, delta_reader, sql_reader, writer)
        load_system_versioned_tables(config, delta_reader, sql_reader, writer)

    if config.mode == LaunchModes.DEBEZIUM:
        load_debezium_tables(config, writer)

