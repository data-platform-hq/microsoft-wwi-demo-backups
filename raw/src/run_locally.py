import logging
import os
from pathlib import Path

import arrow
import click
from dotenv import load_dotenv

from helpers import local_spark_session_init
from raw_microsoft_wwi.configuration import IncrementalLoadConfig, StreamingConfig
from raw_microsoft_wwi.reader import SqlReader, DeltaReader
from raw_microsoft_wwi.writer import Writer
from raw_microsoft_wwi_fixtures_generator.writer.writer import WriterCSV
from helpers import create_debezium_connector, get_debezium_settings
from raw_microsoft_wwi.configuration import LaunchModes, StreamingSources

load_dotenv(Path(__file__).parent / '../.env')

logging.getLogger().setLevel(os.getenv('APP_LOG_MODE'))


@click.group()
def cli():
    pass


@cli.command(name='load', help='Raw data loading')
@click.pass_context
@click.option('-t', '--load_datetime',
              type=str,
              default=None,
              help='DateTime for incremental loading. Use date format: `2014` or `2014-01` or `2014-01-01`')
@click.option('-m', '--load_mode',
              type=LaunchModes,
              show_default=True,
              default=LaunchModes.INCREMENTAL,
              help='Mode of loading data from source')
@click.option('-s', '--streaming_source',
              type=StreamingSources,
              show_default=True,
              default=StreamingSources.KAFKA,
              help='Source for streaming')
@click.option('--create_connector', is_flag=True, show_default=True, default=False, help='Create debezium connector')
@click.option('-c', '--csv_for_test', is_flag=True, show_default=True, default=False, help='Load CSV for test')
@click.option('-l', '--limit', type=int, default=100, help='CSV limit rows')
@click.argument('delta_tables_dest', envvar='DELTA_TABLES_DEST')
@click.argument('test_tables_dest', envvar='TEST_TABLES_DEST')
@click.argument('databricks_catalog', envvar='DATABRICKS_CATALOG')
@click.argument('databricks_schema', envvar='DATABRICKS_SCHEMA')
@click.argument('py4j_log_mode', envvar='PY4J_LOG_MODE')
@click.argument('spark_log_mode', envvar='SPARK_LOG_MODE')
@click.argument('mssql_url', envvar='MSSQL_URL')
@click.argument('mssql_port', envvar='MSSQL_PORT')
@click.argument('mssql_db_name', envvar='MSSQL_DB_NAME')
@click.argument('mssql_user_name', envvar='MSSQL_SA_USER_NAME')
@click.argument('mssql_password', envvar='MSSQL_SA_PASSWORD')
@click.argument('kafka_url', envvar='KAFKA_URL')
@click.argument('kafka_port', envvar='KAFKA_PORT')
@click.argument('kafka_connect_url', envvar='KAFKA_CONNECT_URL')
@click.argument('kafka_connect_port', envvar='KAFKA_CONNECT_PORT')
@click.argument('event_hubs_namespace', envvar='EVENT_HUBS_NAMESPACE')
@click.argument('event_hubs_access_key_name', envvar='EVENT_HUBS_ACCESS_KEY_NAME')
@click.argument('event_hubs_access_key', envvar='EVENT_HUBS_ACCESS_KEY')
@click.argument('consumer_group', envvar='CONSUMER_GROUP')
def loading(ctx: click.Context,
            load_datetime,
            load_mode,
            streaming_source,
            create_connector,
            csv_for_test,
            limit,
            delta_tables_dest,
            test_tables_dest,
            py4j_log_mode,
            spark_log_mode,
            databricks_catalog,
            databricks_schema,
            mssql_url,
            mssql_port,
            mssql_db_name,
            mssql_user_name,
            mssql_password,
            kafka_url,
            kafka_port,
            kafka_connect_url,
            kafka_connect_port,
            event_hubs_namespace,
            event_hubs_access_key_name,
            event_hubs_access_key,
            consumer_group
            ):
    if load_datetime:
        try:
            date_to = arrow.get(load_datetime).datetime
            click.secho(f'Parsed --to param to {date_to}')

        except arrow.parser.ParserError:
            click.secho(f'Cant parse --to parameter: {load_datetime}', fg='red')
            ctx.exit(1)
    elif load_datetime is None and load_mode == LaunchModes.INCREMENTAL:
        raise ValueError("Load datetime cant be None")

    local_spark_session_init(py4j_log_mode, spark_log_mode)
    regular_tables = [
        'Application_SystemParameters',
        'Sales_Invoices',
        'Sales_InvoiceLines',
        'Sales_OrderLines',
        'Sales_Orders',
        'Sales_SpecialDeals',
        'Warehouse_StockItemHoldings',
        'Warehouse_StockItemStockGroups',
        'Warehouse_StockItemTransactions',
        'Purchasing_PurchaseOrders',
        'Purchasing_PurchaseOrderLines'
    ]
    system_versioned_tables = [
        'Application_Cities',
        'Application_Countries',
        'Application_DeliveryMethods',
        'Application_PaymentMethods',
        'Application_People',
        'Application_StateProvinces',
        'Application_TransactionTypes',
        'Sales_BuyingGroups',
        'Sales_CustomerCategories',
        'Sales_Customers',
        'Warehouse_ColdRoomTemperatures',
        'Warehouse_Colors',
        'Warehouse_PackageTypes',
        'Warehouse_StockGroups',
        'Warehouse_StockItems',
        'Purchasing_SupplierCategories',
        'Purchasing_Suppliers',
    ]

    config = IncrementalLoadConfig(delta_tables_dest, regular_tables,
                                   system_versioned_tables, databricks_catalog, databricks_schema,
                                   f'jdbc:sqlserver://'
                                   f'{mssql_url}:{mssql_port};'
                                   f'databaseName={mssql_db_name};'
                                   f'user={mssql_user_name};'
                                   f'password={mssql_password}',
                                   run_mode="default",
                                   e2e_expected_data_delta_path="stub")

    if csv_for_test:
        from raw_microsoft_wwi_fixtures_generator.app import main

        config.input_delta_tables_path = test_tables_dest

        path_to_raw_microsoft_wwi_module = Path(__file__).parent / 'raw_microsoft_wwi'

        config.queries_path = f'{str(path_to_raw_microsoft_wwi_module)}/queries'
        config.schemas_path = f'{str(path_to_raw_microsoft_wwi_module)}/schemas'

        writer = WriterCSV(config, limit)

    else:
        from raw_microsoft_wwi.app import main
        if load_mode == LaunchModes.DEBEZIUM:
            if streaming_source == StreamingSources.KAFKA:
                config = StreamingConfig(delta_tables_dest, regular_tables,
                                        system_versioned_tables, databricks_catalog, databricks_schema,
                                        StreamingSources.KAFKA, f"{kafka_url}:{kafka_port}")
                if create_connector:
                    tables_list, schemas_list = get_debezium_settings(regular_tables + system_versioned_tables)
                    create_debezium_connector(mssql_port, mssql_db_name, mssql_user_name, mssql_password, kafka_url, kafka_port,
                                              kafka_connect_port, kafka_connect_url, schemas_list, tables_list)

            if streaming_source == StreamingSources.EVENTHUB:
                config = StreamingConfig(delta_tables_dest, regular_tables,
                                        system_versioned_tables, databricks_catalog, databricks_schema,
                                        StreamingSources.EVENTHUB, event_hubs_namespace,
                                        event_hubs_access_key_name, event_hubs_access_key,
                                        consumer_group if consumer_group is not None else '$Default')

            config.system_versioned_tables.remove("Warehouse_ColdRoomTemperatures") # table doesn't support cdc

        writer = Writer(config)

    main(sql_reader=SqlReader(config), delta_reader=DeltaReader(config), writer=writer, config=config,
         load_datetime=load_datetime)


if __name__ == '__main__':
    cli()
