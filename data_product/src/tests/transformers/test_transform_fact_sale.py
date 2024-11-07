import pytest

from data_product_microsoft_wwi.transformers import transform_city, transform_employee, \
    transform_stock_item, transform_customer, transform_fact_sale
from tests.conftest import DEST_SCHEMAS, SRS_SCHEMAS, fixtures_path
from tests.helpers import CSVReader


@pytest.fixture(params=[
    (['cities', 'Application/Cities', SRS_SCHEMAS['Cities']],
     ['countries', 'Application/Countries', SRS_SCHEMAS['Countries']],
     ['state_provinces', 'Application/StateProvinces', SRS_SCHEMAS['StateProvinces']],
     ['customers', 'Sales/Customers', SRS_SCHEMAS['Customers']],
     ['customer_categories', 'Sales/CustomerCategories', SRS_SCHEMAS['CustomerCategories']],
     ['buying_groups', 'Sales/BuyingGroups', SRS_SCHEMAS['BuyingGroups']],
     ['people', 'Application/People', SRS_SCHEMAS['People']],
     ['colors', 'Warehouse/Colors', SRS_SCHEMAS['Colors']],
     ['stock_groups', 'Warehouse/StockGroups', SRS_SCHEMAS['StockGroups']],
     ['stock_items', 'Warehouse/StockItems', SRS_SCHEMAS['StockItems']],
     ['stock_item_stock_groups', 'Warehouse/StockItemStockGroups', SRS_SCHEMAS['StockItemStockGroups']],
     ['package_types', 'Warehouse/PackageTypes', SRS_SCHEMAS['PackageTypes']],
     ['invoice_lines', 'Sales/InvoiceLines', SRS_SCHEMAS['InvoiceLines']],
     ['invoices', 'Sales/Invoices', SRS_SCHEMAS['Invoices']])
])
def required_dimensions_fixture(spark_session, request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


def test_check_schema(required_dimensions_fixture):
    fact_sale_transformed = transform_fact_sale(
        invoice_lines=required_dimensions_fixture['invoice_lines'],
        invoices=required_dimensions_fixture['invoices'],
        customers=required_dimensions_fixture['customers'],
        stock_items=required_dimensions_fixture['stock_items'],
        package_types=required_dimensions_fixture['package_types'],
        dimension_employee=transform_employee(required_dimensions_fixture['people']),
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

    )

    assert DEST_SCHEMAS['Sale'] == fact_sale_transformed.schema


def test_sale_key_range(required_dimensions_fixture):
    fact_sale_transformed_key = transform_fact_sale(
        invoice_lines=required_dimensions_fixture['invoice_lines'],
        invoices=required_dimensions_fixture['invoices'],
        customers=required_dimensions_fixture['customers'],
        stock_items=required_dimensions_fixture['stock_items'],
        package_types=required_dimensions_fixture['package_types'],
        dimension_employee=transform_employee(required_dimensions_fixture['people']),
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

    ).select('SaleKey')

    rows = []
    [rows.append(row[0]) for row in fact_sale_transformed_key.collect()]

    assert rows == [*range(0, len(rows), 1)]


def test_empty_df(required_dimensions_fixture):
    fact_sale_transformed = transform_fact_sale(
        invoice_lines=required_dimensions_fixture['invoice_lines'],
        invoices=required_dimensions_fixture['invoices'],
        customers=required_dimensions_fixture['customers'],
        stock_items=required_dimensions_fixture['stock_items'],
        package_types=required_dimensions_fixture['package_types'],
        dimension_employee=transform_employee(required_dimensions_fixture['people']),
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

    )

    assert fact_sale_transformed.count() > 0
