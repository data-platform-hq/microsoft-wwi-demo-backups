import datetime
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from raw_microsoft_wwi.configuration import Config
from raw_microsoft_wwi.reader import SqlReader
from raw_microsoft_wwi.reader.delta_reader import DeltaReader
from raw_microsoft_wwi.transformer.transformer import Transformer
from raw_microsoft_wwi_fixtures_generator.writer.writer import WriterCSV

ORDER_BY_DICT = {
        'Application_SystemParameters' : ['SystemParameterID'],
        'Sales_Invoices' : ['InvoiceID'],
        'Sales_InvoiceLines' : ['InvoiceID'],
        'Sales_OrderLines' : ['OrderLineID'],
        'Sales_Orders' : ['OrderID'],
        'Sales_SpecialDeals' : ['SpecialDealID'],
        'Warehouse_StockItemHoldings' : ['StockItemID'],
        'Warehouse_StockItemStockGroups' : ['StockItemID'],
        'Warehouse_StockItemTransactions' : ['StockItemID'],
        'Application_Cities' : ['CityID'],
        'Application_Countries' : ['LastEditedBy','Region'],
        'Application_DeliveryMethods' : ['DeliveryMethodID'],
        'Application_PaymentMethods' : ['PaymentMethodID'],
        'Application_People' : ['IsPermittedToLogon','PersonID'],
        'Application_StateProvinces' : ['LastEditedBy','StateProvinceID'],
        'Application_TransactionTypes' : ['TransactionTypeID'],
        'Sales_BuyingGroups' : ['BuyingGroupID'],
        'Sales_CustomerCategories' : ['CustomerCategoryID'],
        'Sales_Customers' : ['PrimaryContactPersonID'],
        'Warehouse_ColdRoomTemperatures' : ['ColdRoomTemperatureID'],
        'Warehouse_Colors' : ['ColorID'],
        'Warehouse_PackageTypes': ['PackageTypeID'],
        'Warehouse_StockGroups' : ['StockGroupID'],
        'Warehouse_StockItems' : ['StockItemID']
}


def get_or_create_table(config, table_name, table_type):
    spark = SparkSession.getActiveSession()
    schema_file = open(
        os.path.join(
            config.schemas_path,
            f"{table_type}_tables_schemas",
            f"{table_name}.json"
        ), 'r'
    )
    schema = StructType.fromJson(json.loads(schema_file.read()))
    empty_df = spark.createDataFrame([], schema)
    return empty_df


def load_regular_tables(config, delta_reader, sql_reader, writer, order_by):
    load_datetime = os.getenv("INGESTION_DATETIME", datetime.datetime.now())

    for table_name in config.regular_tables:
        query_file = open(
            os.path.join(
                config.queries_path,
                "regular_tables_queries",
                f"{table_name}.sql"
            ), 'r'
        )

        existing_df = get_or_create_table(
            config,
            table_name,
            table_type='regular'
        )

        transformed_query = Transformer.append_query_filter_regular_tables(
            existing_df, query_file.read(), load_datetime
        )

        delta_table = sql_reader.read(transformed_query)
        transformed_table = Transformer.transform_table(delta_table, load_datetime)
        writer.write_table(transformed_table, table_name, order_by[table_name])


def load_system_versioned_tables(config, delta_reader, sql_reader, writer, order_by):
    load_datetime = os.getenv("INGESTION_DATETIME", datetime.datetime.now())

    for table_name in config.system_versioned_tables:
        query_file = open(
            os.path.join(config.queries_path,
                         "system_versioned_tables_queries",
                         f"{table_name}.sql"), 'r'
        )

        existing_df = get_or_create_table(config, table_name, 'system_versioned')

        transformed_query = Transformer.append_filter_by_valid_from_valid_to(
            existing_df, query_file.read(), load_datetime
        )

        delta_table = sql_reader.read(transformed_query)
        transformed_table = Transformer.transform_table(delta_table, load_datetime)
        writer.write_table(transformed_table, table_name, order_by[table_name])


def main(sql_reader: SqlReader, delta_reader: DeltaReader, writer: WriterCSV,
         load_datetime: datetime.datetime, config: Config):
    os.environ["INGESTION_DATETIME"] = str(load_datetime)

    load_regular_tables(config, delta_reader, sql_reader, writer, ORDER_BY_DICT)
    load_system_versioned_tables(config, delta_reader, sql_reader, writer, ORDER_BY_DICT)
