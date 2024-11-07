from datetime import datetime

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers import transform_customer
from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from tests.conftest import DEST_SCHEMAS, SRS_SCHEMAS, fixtures_path
from tests.helpers import CSVReader

HEAD_ROW_STRUCTURE = (0, 0, 'Unknown', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', None, None, 0)


@pytest.fixture(params=[
    (['customers', 'Sales/Customers', SRS_SCHEMAS['Customers']],
     ['customer_categories', 'Sales/CustomerCategories', SRS_SCHEMAS['CustomerCategories']],
     ['buying_groups', 'Sales/BuyingGroups', SRS_SCHEMAS['BuyingGroups']],
     ['people', 'Application/People', SRS_SCHEMAS['People']])
])
def required_dimensions_fixture(spark_session, request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


def test_check_schema(required_dimensions_fixture):
    customer = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people']
    )

    assert DEST_SCHEMAS['Customer'] == customer.schema


def test_check_first_row_if_offset_is_0(required_dimensions_fixture):
    spark_session = SparkSession.getActiveSession()

    customer = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people']
    )

    excepted_df = (spark_session.createDataFrame([HEAD_ROW_STRUCTURE], DEST_SCHEMAS['Customer'])
                   .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                   .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS")))

    assert excepted_df.head() == customer.head()


def test_customer_key_range(required_dimensions_fixture):
    customers_transformed_customer_key = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people']
    ).select('CustomerKey')

    rows = []
    [rows.append(row[0]) for row in customers_transformed_customer_key.collect()]

    assert rows == [*range(0, len(rows), 1)]


def test_customer_coincidence_id_name(required_dimensions_fixture):
    customers_transformed = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people'])

    customers_transformed_dist_id = customers_transformed.select('WWICustomerID', 'Customer').distinct() \
        .withColumn("IsDistinct", F.count('WWICustomerID').over(Window.partitionBy("Customer")))

    assert customers_transformed_dist_id.agg({"IsDistinct": "max"}).collect()[0][0] < 2


@pytest.mark.skip(reason="test hase en error on remote env")
def test_customer_valid_to_filter(required_dimensions_fixture):
    """
    Check ValidFrom ValidTo filtering
    """
    customers_transformed = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people']
    )
    assert 0 == customers_transformed.select().filter(F.col('ValidTo') <= F.col('ValidFrom')).count()


def test_customer_date_to(required_dimensions_fixture):
    to = datetime(year=2014, month=3, day=1)
    customers_transformed = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people'],
        to=to
    )

    assert customers_transformed.agg({"ValidFrom": "max"}).collect()[0][0] <= to


def test_empty_df(required_dimensions_fixture):
    customers_transformed = transform_customer(
        customers=required_dimensions_fixture['customers'],
        customer_categories=required_dimensions_fixture['customer_categories'],
        buying_groups=required_dimensions_fixture['buying_groups'],
        people=required_dimensions_fixture['people']
    )

    assert customers_transformed.count() > 1
