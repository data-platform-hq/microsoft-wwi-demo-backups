# Databricks notebook source
# MAGIC %md
# MAGIC Set mandatory constants through widgets above
# MAGIC * load_datetime - specify the year only, for example `2014`, or more precisely base format `2014-01-01T00:00:00`. It will be used datetime.now() in case of blank line.

# COMMAND ----------

dbutils.widgets.text('load_datetime', '2014-01-01T00:00:00')
dbutils.widgets.text('is_e2e', 'False')
dbutils.widgets.dropdown('load_mode', 'incremental', ['incremental', 'debezium'])
dbutils.widgets.dropdown('streaming_source', 'eventhubs', ['eventhubs', 'kafka'])
dbutils.widgets.text('streaming_consumer_group', '$Default')
dbutils.widgets.text('catalog', 'wwi_demo')
dbutils.widgets.text('schema', 'raw')
dbutils.widgets.text('raw_area_path', 'raw')

# COMMAND ----------

import datetime

load_datetime_input = dbutils.widgets.get('load_datetime')

formats = {4: '%Y', 19: '%Y-%m-%dT%H:%M:%S'}
load_datetime = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"

try:
    if len(load_datetime_input) > 0:
        load_datetime = datetime.datetime.strptime(load_datetime_input, formats[len(load_datetime_input)])
        now = datetime.datetime.now()
        if load_datetime > now:
            load_datetime = now
except (NameError, ValueError, KeyError, TypeError) as err:
    raise ValueError("Wrong format or symbol(s). Correct examples: '2014' or '2014-01-01T00:00:00' or leave blank to use datetime.now()") from err

print(load_datetime)

# COMMAND ----------

IS_E2E = eval(dbutils.widgets.get('is_e2e'))
load_mode = dbutils.widgets.get('load_mode')
catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
run_mode = "default" if not IS_E2E or load_mode == 'debezium' else "e2e_test"

if run_mode == "e2e_test":
    print("Running in E2E mode")
else:
    print("Running in default node")

# COMMAND ----------

from raw_microsoft_wwi.configuration import StreamingSources, IncrementalLoadConfig, StreamingConfig
from raw_microsoft_wwi.reader import SqlReader, DeltaReader
from raw_microsoft_wwi.writer.writer import Writer
from raw_microsoft_wwi.app import main
from raw_microsoft_wwi.e2e_tests.e2e import run_e2e_workflow

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
    'Purchasing_PurchaseOrderLines',
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

raw_area_path = dbutils.widgets.get('raw_area_path') if not run_mode == "e2e_test" \
    else dbutils.secrets.get(scope="external", key="raw-e2e-path")

if load_mode == 'debezium':
    streaming_source = dbutils.widgets.get('streaming_source')
    if streaming_source == 'eventhubs':
        streaming_source_url = dbutils.secrets.get(scope="external", key="namespace-endpoint")
        event_hubs_access_key = dbutils.secrets.get(scope="external", key="namespace-primary-key")
        event_hubs_access_key_name = dbutils.secrets.get(scope="external", key="namespace-key-name")
        consumer_group = dbutils.widgets.get('streaming_consumer_group')
        config = StreamingConfig(f"{raw_area_path}/microsoft-wwi", regular_tables, system_versioned_tables,
                                 catalog, schema, StreamingSources.EVENTHUB, streaming_source_url,
                                 event_hubs_access_key_name, event_hubs_access_key, consumer_group)
    elif streaming_source == 'kafka':
        streaming_source_url = 'localhost:9092' #kafka broker url
        config = StreamingConfig(f"{raw_area_path}/microsoft-wwi", regular_tables, system_versioned_tables,
                                 catalog, schema, StreamingSources.KAFKA, streaming_source_url)
    else:
        raise ValueError(f"Unsupported streaming_source value: {streaming_source}")
elif load_mode == 'incremental': 
    db_url = dbutils.secrets.get(scope="external", key="wwi-demo-server-url")
    db_username = dbutils.secrets.get(scope="external", key="wwi-demo-server-admin-username")
    db_password = dbutils.secrets.get(scope="external", key="wwi-demo-server-admin-login-password")
    e2e_expected_data_delta_path = f'{dbutils.secrets.get(scope="external", key="e2e-expected-data-path")}/raw' if run_mode == "e2e_test" else ""

    config = IncrementalLoadConfig(input_delta_tables_path=f"{raw_area_path}/microsoft-wwi",
                                   regular_tables=regular_tables, system_versioned_tables=system_versioned_tables,
                                   catalog=catalog, schema=schema, run_mode=run_mode,
                                   e2e_expected_data_delta_path=f"{e2e_expected_data_delta_path}/microsoft-wwi",
                                   source_database_jdbc_url=f"{db_url};user={db_username};password={db_password};")
else:
    raise ValueError(f"Unsupported load_mode value: {load_mode}")

# COMMAND ----------
main(
    sql_reader=SqlReader(config),
    delta_reader=DeltaReader(config),
    writer=Writer(config),
    load_datetime=load_datetime,
    config=config
)

if config.run_mode == "e2e_test":
    run_e2e_workflow(config=config)
