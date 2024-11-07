import logging
import os
from pathlib import Path

import arrow
import click
from dotenv import load_dotenv
from py4j.protocol import Py4JJavaError

from data_product_microsoft_wwi.app import TransformerFactory, TransformerType, TransformationType
from data_product_microsoft_wwi.configuration import merge_keys_names, output_table_name_path, \
    output_transformed_tables_primary_keys_names, OutputDimensionsTableNames, OutputFactTableName
from data_product_microsoft_wwi.helpers import retry
from data_product_microsoft_wwi.loaders import SaveType
from helpers import local_spark_session_init

load_dotenv(Path(__file__).parent / '../.env')

logging.getLogger().setLevel(os.getenv('APP_LOG_MODE'))


@click.group()
def cli():
    pass


@cli.command(name='load', help='Full reload data')
@click.pass_context
@click.option('-t', '--to', type=str, default=None, help='DateTime to incremental loading')
@click.option('-m', '--max_attempts', type=int, default=3, help='DateTime to incremental loading')
@click.option('-b', '--backoff_factor', type=float, default=1.5, help='DateTime to incremental loading')
@click.option('-c', '--calendar_start_date', type=str, default='2000-01-01', help='Calendar start date')
@click.option('-d', '--destination',
              type=click.Choice(['delta', 'sql', 'synapse', 'snowflake', 'all']),
              multiple=True,
              default=['all'],
              show_default=True,
              help='Destinations loading')
@click.argument('delta_tables_srs', envvar='DELTA_TABLES_SRS')
@click.argument('delta_tables_dest', envvar='DELTA_TABLES_DEST')
@click.argument('databricks_schema', envvar='DATABRICKS_SCHEMA')
@click.argument('py4j_log_mode', envvar='PY4J_LOG_MODE')
@click.argument('spark_log_mode', envvar='SPARK_LOG_MODE')
@click.argument('mssql_url', envvar='MSSQL_URL')
@click.argument('mssql_port', envvar='MSSQL_PORT')
@click.argument('mssql_db_name', envvar='MSSQL_DB_NAME')
@click.argument('mssql_user_name', envvar='MSSQL_USER_NAME')
@click.argument('mssql_password', envvar='MSSQL_PASSWORD')
@click.argument('synapse_url', envvar='SYNAPSE_URL')
@click.argument('synapse_temp_dir', envvar='SYNAPSE_TEMP_DIR')
@click.argument('snowflake_url', envvar='SNOWFLAKE_URL')
@click.argument('snowflake_user', envvar='SNOWFLAKE_USER')
@click.argument('snowflake_password', envvar='SNOWFLAKE_KEY')
@click.argument('snowflake_database', envvar='SNOWFLAKE_DATABASE')
@click.argument('snowflake_schema', envvar='SNOWFLAKE_SCHEMA')
@click.argument('snowflake_warehouse', envvar='SNOWFLAKE_WAREHOUSE')
def product_data_loading(ctx: click.Context,
                         to,
                         max_attempts,
                         backoff_factor,
                         calendar_start_date,
                         destination,
                         delta_tables_srs,
                         delta_tables_dest,
                         databricks_schema,
                         py4j_log_mode,
                         spark_log_mode,
                         mssql_url,
                         mssql_port,
                         mssql_db_name,
                         mssql_user_name,
                         mssql_password,
                         synapse_url,
                         synapse_temp_dir,
                         snowflake_url,
                         snowflake_user,
                         snowflake_password,
                         snowflake_database,
                         snowflake_schema,
                         snowflake_warehouse):
    """
    Load product data
    """

    if to:
        try:
            click.secho('Try to make incremental loading')
            date_to = arrow.get(to).datetime
            click.secho(f'Parsed you --to param to {date_to}')

        except arrow.parser.ParserError:
            click.secho(f'Cant parse --to parameter. {to}', fg='red')
            ctx.exit(1)
    else:
        click.secho('Try to make full reloading')
        date_to = None

    transformer_factory = TransformerFactory(date_to)

    _ = local_spark_session_init(py4j_log_mode, spark_log_mode)

    loading_start_time = arrow.utcnow()

    try:

        dimensions_save_list = [
            OutputDimensionsTableNames.CITY,
            OutputDimensionsTableNames.CUSTOMER,
            OutputDimensionsTableNames.EMPLOYEE,
            OutputDimensionsTableNames.CALENDAR,
            OutputDimensionsTableNames.STOCK_ITEM,
            OutputDimensionsTableNames.TRANSACTION_TYPE,
            OutputDimensionsTableNames.SUPPLIER,
            OutputDimensionsTableNames.PAYMENT_METHOD,
        ]

        facts_save_list = [
            OutputFactTableName.SALE,
            OutputFactTableName.STOCK_HOLDING,
            OutputFactTableName.ORDER,
        ]

        retry_decorator = retry(max_attempts=max_attempts, backoff_factor=backoff_factor,
                                expected_exceptions=[Py4JJavaError])

        if 'delta' in destination or 'all' in destination:
            delta_transformer = transformer_factory.get_transformer(TransformerType.DELTA,
                                                                    delta_tables_srs=delta_tables_srs,
                                                                    delta_tables_dest=delta_tables_dest,
                                                                    output_table_name_paths=output_table_name_path,
                                                                    databricks_schema=databricks_schema,
                                                                    merge_keys_names=merge_keys_names,
                                                                    output_transformed_tables_primary_keys_names=output_transformed_tables_primary_keys_names,
                                                                    start_date=arrow.get(calendar_start_date).date()
                                                                    )
            delta_transformer.transform(TransformationType.DIMENSIONS)
            delta_transformer.save(SaveType.OVERWRITE, dimensions_save_list)
            delta_transformer.transform(TransformationType.FACTS, date_to)
            delta_transformer.save(SaveType.MERGE if date_to else SaveType.OVERWRITE, facts_save_list)

        if 'sql' in destination or 'all' in destination:
            azure_transformer = transformer_factory.get_transformer(TransformerType.SQL,
                                                                    delta_tables_srs=delta_tables_srs,
                                                                    delta_tables_dest=delta_tables_dest,
                                                                    db_url=f'jdbc:sqlserver://{mssql_url}:{mssql_port};database={mssql_db_name}',
                                                                    user=mssql_user_name,
                                                                    password=mssql_password,
                                                                    output_table_name_paths=output_table_name_path,
                                                                    merge_keys_names=merge_keys_names,
                                                                    start_date=arrow.get(calendar_start_date).date()
                                                                    )

            azure_transformer.transform(TransformationType.DIMENSIONS)
            retry_decorator(azure_transformer.save)(SaveType.OVERWRITE, dimensions_save_list)

            azure_transformer.transform(TransformationType.FACTS, date_to)
            retry_decorator(azure_transformer.save)(SaveType.APPEND if date_to else SaveType.OVERWRITE,
                                                    facts_save_list)

        if 'synapse' in destination or 'all' in destination:
            synapse_transformer = transformer_factory.get_transformer(TransformerType.SYNAPSE,
                                                                      delta_tables_srs=delta_tables_srs,
                                                                      delta_tables_dest=delta_tables_dest,
                                                                      db_url=synapse_url,
                                                                      user=mssql_user_name,
                                                                      password=mssql_password,
                                                                      temp_dir=synapse_temp_dir,
                                                                      output_table_name_paths=output_table_name_path,
                                                                      merge_keys_names=merge_keys_names,
                                                                      start_date=arrow.get(calendar_start_date).date()
                                                                      )

            synapse_transformer.transform(TransformationType.DIMENSIONS)
            retry_decorator(synapse_transformer.save)(SaveType.OVERWRITE, dimensions_save_list)

            synapse_transformer.transform(TransformationType.FACTS, date_to)
            retry_decorator(synapse_transformer.save)(SaveType.APPEND if date_to else SaveType.OVERWRITE,
                                                      facts_save_list)

        if 'snowflake' in destination or 'all' in destination:
            snowflake_transformer = transformer_factory.get_transformer(TransformerType.SNOWFLAKE,
                                                                        delta_tables_srs=delta_tables_srs,
                                                                        delta_tables_dest=delta_tables_dest,
                                                                        snowflake_url=snowflake_url,
                                                                        snowflake_user=snowflake_user,
                                                                        snowflake_password=snowflake_password,
                                                                        snowflake_database=snowflake_database,
                                                                        snowflake_schema=snowflake_schema,
                                                                        snowflake_warehouse=snowflake_warehouse,
                                                                        output_table_name_paths=output_table_name_path,
                                                                        merge_keys_names=merge_keys_names,
                                                                        start_date=arrow.get(calendar_start_date).date()
                                                                        )

            snowflake_transformer.transform(TransformationType.DIMENSIONS)
            retry_decorator(snowflake_transformer.save)(SaveType.OVERWRITE, dimensions_save_list)

            snowflake_transformer.transform(TransformationType.FACTS, date_to)
            retry_decorator(snowflake_transformer.save)(SaveType.APPEND if date_to else SaveType.OVERWRITE,
                                                        facts_save_list)

    except Exception as e:
        # Just for debugging, catch and analyze errors
        raise

    loading_end_time = arrow.utcnow()

    click.echo(f'Loading has been finished during {str(loading_end_time - loading_start_time)}')


if __name__ == '__main__':
    cli()
