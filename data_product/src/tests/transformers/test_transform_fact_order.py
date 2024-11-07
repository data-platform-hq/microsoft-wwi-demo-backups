import pytest
from datetime import datetime
from pyspark.sql.functions import *

from data_product_microsoft_wwi.transformers import transform_fact_order, transform_city, transform_customer, \
    transform_stock_item, transform_employee
from tests.conftest import SRS_SCHEMAS, DEST_SCHEMAS, fixtures_path
from tests.helpers import CSVReader


@pytest.fixture(params=[
    (['sales_orders', 'Sales/Orders', SRS_SCHEMAS['SalesOrders']],
     ['sales_orderlines', 'Sales/OrderLines', SRS_SCHEMAS['SalesOrderLines']],
     ['warehouse_packagetypes', 'Warehouse/PackageTypes', SRS_SCHEMAS['WarehousePackageTypes']],
     ['customers', 'Sales/Customers', SRS_SCHEMAS['Customers']],
     ['cities', 'Application/Cities', SRS_SCHEMAS['Cities']],
     ['countries', 'Application/Countries', SRS_SCHEMAS['Countries']],
     ['state_provinces', 'Application/StateProvinces', SRS_SCHEMAS['StateProvinces']],
     ['customer_categories', 'Sales/CustomerCategories', SRS_SCHEMAS['CustomerCategories']],
     ['buying_groups', 'Sales/BuyingGroups', SRS_SCHEMAS['BuyingGroups']],
     ['people', 'Application/People', SRS_SCHEMAS['People']],
     ['colors', 'Warehouse/Colors', SRS_SCHEMAS['Colors']],
     ['stock_groups', 'Warehouse/StockGroups', SRS_SCHEMAS['StockGroups']],
     ['stock_items', 'Warehouse/StockItems', SRS_SCHEMAS['StockItems']],
     ['stock_item_stock_groups', 'Warehouse/StockItemStockGroups', SRS_SCHEMAS['StockItemStockGroups']],
     ['package_types', 'Warehouse/PackageTypes', SRS_SCHEMAS['PackageTypes']]
     )
])
def required_dimensions_fixture(spark_session, request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


def test_fact_order_schema(required_dimensions_fixture):
    fact_order_df = transform_fact_order(
        sales_orders=required_dimensions_fixture["sales_orders"],
        sales_orderlines=required_dimensions_fixture["sales_orderlines"],
        warehouse_package_types=required_dimensions_fixture["warehouse_packagetypes"],
        sales_customers=required_dimensions_fixture["customers"],
        dimension_city=transform_city(
            cities=required_dimensions_fixture['cities'],
            countries=required_dimensions_fixture['countries'],
            state_provinces=required_dimensions_fixture['state_provinces']
        ),
        dimension_customer=transform_customer(
            customers=required_dimensions_fixture['customers'],
            customer_categories=required_dimensions_fixture['customer_categories'],
            buying_groups=required_dimensions_fixture['buying_groups'],
            people=required_dimensions_fixture['people']
        ),
        dimension_stock_item=transform_stock_item(
            colors=required_dimensions_fixture['colors'],
            stock_groups=required_dimensions_fixture['stock_groups'],
            stock_items=required_dimensions_fixture['stock_items'],
            stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
            package_types=required_dimensions_fixture['package_types']
        ),
        dimension_employee=transform_employee(required_dimensions_fixture['people'])
    )

    assert fact_order_df.schema == DEST_SCHEMAS["FactOrder"]


def test_fact_order_date_filters(required_dimensions_fixture):
    date_time_since = datetime.strptime("2013-12-31 23:00:00", "%Y-%m-%d %H:%M:%S")
    load_to = datetime.strptime("2014-01-01 01:00:00", "%Y-%m-%d %H:%M:%S")

    fact_order_df = transform_fact_order(
        sales_orders=required_dimensions_fixture["sales_orders"],
        sales_orderlines=required_dimensions_fixture["sales_orderlines"],
        warehouse_package_types=required_dimensions_fixture["warehouse_packagetypes"],
        sales_customers=required_dimensions_fixture["customers"],
        dimension_city=transform_city(
            cities=required_dimensions_fixture['cities'],
            countries=required_dimensions_fixture['countries'],
            state_provinces=required_dimensions_fixture['state_provinces']
        ),
        dimension_customer=transform_customer(
            customers=required_dimensions_fixture['customers'],
            customer_categories=required_dimensions_fixture['customer_categories'],
            buying_groups=required_dimensions_fixture['buying_groups'],
            people=required_dimensions_fixture['people']
        ),
        dimension_stock_item=transform_stock_item(
            colors=required_dimensions_fixture['colors'],
            stock_groups=required_dimensions_fixture['stock_groups'],
            stock_items=required_dimensions_fixture['stock_items'],
            stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
            package_types=required_dimensions_fixture['package_types']
        ),
        dimension_employee=transform_employee(required_dimensions_fixture['people']),
        since=date_time_since,
        to=load_to
    )

    assert fact_order_df.filter((col("RawDataLoadDatetime") < date_time_since) |
                                (col("RawDataLoadDatetime") > load_to)).count() == 0
