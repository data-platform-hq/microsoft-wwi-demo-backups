# Databricks notebook source
# MAGIC %md
# MAGIC Set config variables through widgets
# MAGIC * catalog - Databricks catalog where data ingested with raw-microsoft-wwi is located and to which data product tables will be written
# MAGIC * destination - choose a kind of destination loading. Default is 'delta'
# MAGIC * is_e2e - notebook run-mode. Could be True (e2e mode) or False (default mode).
# MAGIC * load_datetime - up to time for incremental loading. The format may be the year `2014` only, or more precisely `2014-01-01T00:00:00`. Default is ''
# MAGIC * source_tables_schema - Databricks schema where data ingested with raw-microsoft-wwi is located

# COMMAND ----------

import arrow
import datetime

dbutils.widgets.text("load_datetime", "")
dbutils.widgets.text("is_e2e", "False")
dbutils.widgets.dropdown("destination", "delta", ["delta"])
dbutils.widgets.text("catalog", "wwi_demo")
dbutils.widgets.text("destination_schema", "02_data_product_microsoft_wwi")
dbutils.widgets.text("source_tables_schema", "raw_microsoft_wwi")
dbutils.widgets.text('raw_area_path', 'raw')
dbutils.widgets.text('data_product_area_path', 'data_product')

load_datetime = dbutils.widgets.get("load_datetime")
catalog = dbutils.widgets.get("catalog")
source_tables_schema = dbutils.widgets.get("source_tables_schema")

DESTINATIONS = dbutils.widgets.get("destination")

formats = {4: '%Y', 19: '%Y-%m-%dT%H:%M:%S'}
DATE_TO = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"

try:
    if len(load_datetime) > 0:
        DATE_TO = datetime.datetime.strptime(load_datetime, formats[len(load_datetime)])
        now = datetime.datetime.now()
        if DATE_TO > now:
            DATE_TO = now
except (NameError, ValueError, KeyError, TypeError) as err:
    raise ValueError("Wrong format or symbol(s). Correct examples: '2014' or '2014-01-01T00:00:00' or leave blank to use datetime.now()") from err

IS_E2E = eval(dbutils.widgets.get("is_e2e"))

run_mode = "default" if not IS_E2E else "e2e_test"
if run_mode == "e2e_test":
    print("Running in E2E mode")
else:
    print("Running in default node")

DATABRICKS_SCHEMA = (
    dbutils.widgets.get("destination_schema") if not run_mode == "e2e_test" else
    "data_product_microsoft_wwi_actual_e2e_v2"
)
LOGGING_LEVEL = "WARNING"

# Set attempts and back off factor
MAX_ATTEMPTS = 3
BACKOFF_FACTOR = 1.5
CALENDAR_START_DATE_STR = "2000-01-01"

RAW_AREA_PATH = (
    dbutils.widgets.get('raw_area_path') if not run_mode == "e2e_test" else
    dbutils.secrets.get(scope="external", key="raw-e2e-path")
)
DATA_PRODUCT_AREA_PATH = (
    dbutils.widgets.get('data_product_area_path') if not run_mode == "e2e_test" else
    dbutils.secrets.get(scope="external", key="data-product-e2e-path")
)

INPUT_DELTA_TABLES_PATH = f"{RAW_AREA_PATH}/microsoft-wwi/"
OUTPUT_DELTA_TABLES_PATH = f"{DATA_PRODUCT_AREA_PATH}/microsoft-wwi/"

E2E_EXPECTED_DATA_PATH = (
    f"{dbutils.secrets.get(scope='external', key='e2e-expected-data-path')}"
    f"/data-product/microsoft-wwi/"
) if run_mode == "e2e_test" else ""

# COMMAND ----------

# MAGIC %md
# MAGIC Import required libs and set logging level.
# MAGIC Set path varaibles from vault

# COMMAND ----------

from data_product_microsoft_wwi.app import TransformerFactory, TransformerType, TransformationType
from data_product_microsoft_wwi.configuration import merge_keys_names, output_table_name_path, \
    OutputDimensionsTableNames, OutputFactTableName
from data_product_microsoft_wwi.helpers import retry
from data_product_microsoft_wwi.loaders import SaveType
from data_product_microsoft_wwi.e2e_tests.e2e import run_e2e_workflow
from py4j.protocol import Py4JJavaError
import logging

logging.getLogger().setLevel(LOGGING_LEVEL)

transformer_factory = TransformerFactory(DATE_TO)
retry_decorator = retry(max_attempts=MAX_ATTEMPTS, backoff_factor=BACKOFF_FACTOR, expected_exceptions=[Py4JJavaError])
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

# COMMAND ----------

# MAGIC %md
# MAGIC Delta Lake loading

# COMMAND ----------

if 'delta' in DESTINATIONS or 'all' in DESTINATIONS:
    delta_transformer = transformer_factory.get_transformer(TransformerType.DELTA,
                                                            source_catalog=catalog, source_schema=source_tables_schema,
                                                            catalog=catalog, schema=DATABRICKS_SCHEMA,
                                                            merge_keys_names=merge_keys_names,
                                                            start_date=arrow.get(CALENDAR_START_DATE_STR).date())
    delta_transformer.transform(TransformationType.DIMENSIONS)
    delta_transformer.save(SaveType.OVERWRITE, dimensions_save_list)
    delta_transformer.transform(TransformationType.FACTS, DATE_TO)
    delta_transformer.save(SaveType.MERGE if DATE_TO else SaveType.OVERWRITE, facts_save_list)

# COMMAND ----------

# MAGIC %md
# MAGIC Azure SQL loading

# COMMAND ----------

if 'sql' in DESTINATIONS or 'all' in DESTINATIONS:
    DB_URL = dbutils.secrets.get(scope="external", key="mssql-wideworldimporters-data-product-database-url")
    DB_USERNAME = dbutils.secrets.get(scope="external", key="mssql-server-databricks-username")
    DB_PASSWORD = dbutils.secrets.get(scope="external", key="mssql-server-databricks-password")

    azure_transformer = transformer_factory.get_transformer(TransformerType.SQL,
                                                            source_catalog=catalog, source_schema=source_tables_schema,
                                                            db_url=DB_URL,
                                                            user=DB_USERNAME,
                                                            password=DB_PASSWORD,
                                                            output_table_name_paths=output_table_name_path,
                                                            merge_keys_names=merge_keys_names,
                                                            start_date=arrow.get(CALENDAR_START_DATE_STR).date()
                                                            )

    azure_transformer.transform(TransformationType.DIMENSIONS)
    retry_decorator(azure_transformer.save)(SaveType.OVERWRITE, dimensions_save_list)

    azure_transformer.transform(TransformationType.FACTS, DATE_TO)
    retry_decorator(azure_transformer.save)(SaveType.APPEND if DATE_TO else SaveType.OVERWRITE, facts_save_list)

# COMMAND ----------

# MAGIC %md
# MAGIC Synapse loading

# COMMAND ----------

if 'synapse' in DESTINATIONS or 'all' in DESTINATIONS:
    SYNAPSE_URL = dbutils.secrets.get(scope="external", key="synapse-dataproductdpafdevwesteurope-database-url")
    SYNAPSE_TEMP_DIR = "abfss://dpaf-data@dpafdevwesteurope.dfs.core.windows.net/data/data-product/synapse_temp"

    synapse_transformer = transformer_factory.get_transformer(TransformerType.SYNAPSE,
                                                              source_catalog=catalog,
                                                              source_schema=source_tables_schema,
                                                              db_url=SYNAPSE_URL,
                                                              user=DB_USERNAME,
                                                              password=DB_PASSWORD,
                                                              temp_dir=SYNAPSE_TEMP_DIR,
                                                              output_table_name_paths=output_table_name_path,
                                                              merge_keys_names=merge_keys_names,
                                                              start_date=arrow.get(CALENDAR_START_DATE_STR).date()
                                                              )

    retry_decorator(synapse_transformer.save)(SaveType.OVERWRITE, dimensions_save_list)

    synapse_transformer.transform(TransformationType.FACTS, DATE_TO)
    retry_decorator(synapse_transformer.save)(SaveType.APPEND if DATE_TO else SaveType.OVERWRITE,
                                              facts_save_list)

# COMMAND ----------

# MAGIC %md
# MAGIC Snowflake loading

# COMMAND ----------

if 'snowflake' in DESTINATIONS or 'all' in DESTINATIONS:
    SNOWFLAKE_DATABASE = "AZURE-WWI-DATABRICKS"
    SNOWFLAKE_SCHEMA = "TEST"
    SNOWFLAKE_URL = dbutils.secrets.get(scope='external', key="snowflake-account-url")
    SNOWFLAKE_USER = dbutils.secrets.get(scope='external', key="snowflake-dev-user")
    SNOWFLAKE_KEY = dbutils.secrets.get(scope='external', key="snowflake-dev-user-password")
    SNOWFLAKE_WAREHOUSE = dbutils.secrets.get(scope='external', key="snowflake-default-warehouse")

    snowflake_transformer = transformer_factory.get_transformer(TransformerType.SNOWFLAKE,
                                                                source_catalog=catalog,
                                                                source_schema=source_tables_schema,
                                                                snowflake_url=SNOWFLAKE_URL,
                                                                snowflake_user=SNOWFLAKE_USER,
                                                                snowflake_password=SNOWFLAKE_KEY,
                                                                snowflake_database=SNOWFLAKE_DATABASE,
                                                                snowflake_schema=SNOWFLAKE_SCHEMA,
                                                                snowflake_warehouse=SNOWFLAKE_WAREHOUSE,
                                                                output_table_name_paths=output_table_name_path,
                                                                merge_keys_names=merge_keys_names,
                                                                start_date=arrow.get(CALENDAR_START_DATE_STR).date()
                                                                )

    retry_decorator(snowflake_transformer.save)(SaveType.OVERWRITE, dimensions_save_list)

    snowflake_transformer.transform(TransformationType.FACTS, DATE_TO)
    retry_decorator(snowflake_transformer.save)(SaveType.APPEND if DATE_TO else SaveType.OVERWRITE,
                                                facts_save_list)

# COMMAND ----------

# MAGIC %md
# MAGIC E2E Test run

# COMMAND ----------

if run_mode == "e2e_test":
    run_e2e_workflow(E2E_EXPECTED_DATA_PATH, OUTPUT_DELTA_TABLES_PATH)
