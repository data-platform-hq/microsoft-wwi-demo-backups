from datetime import datetime
from decimal import Decimal

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers import transform_stock_item
from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from tests.conftest import DEST_SCHEMAS, SRS_SCHEMAS, fixtures_path
from tests.helpers import CSVReader

HEAD_ROW_STRUCTURE = (0, 0, 'Unknown', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 0, 0, False, 'N/A',
                      Decimal(0.000), Decimal(0.000), Decimal(0.000), Decimal(0.000), None, None, None, 0)


@pytest.fixture(params=[
    (['colors', 'Warehouse/Colors', SRS_SCHEMAS['Colors']],
     ['stock_groups', 'Warehouse/StockGroups', SRS_SCHEMAS['StockGroups']],
     ['stock_items', 'Warehouse/StockItems', SRS_SCHEMAS['StockItems']],
     ['stock_item_stock_groups', 'Warehouse/StockItemStockGroups', SRS_SCHEMAS['StockItemStockGroups']],
     ['package_types', 'Warehouse/PackageTypes', SRS_SCHEMAS['PackageTypes']])
])
def required_dimensions_fixture(spark_session, request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


def test_check_schema(required_dimensions_fixture):
    stock_item = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types']
    )

    assert DEST_SCHEMAS['StockItem'] == stock_item.schema


def test_check_dataframe_structure(required_dimensions_fixture):
    spark_session = SparkSession.getActiveSession()

    stock_item_transformed = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types']
    ).orderBy(F.col('StockItemKey'))

    excepted_df = (spark_session.createDataFrame([HEAD_ROW_STRUCTURE], DEST_SCHEMAS['StockItem'])
                   .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                   .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS")))

    assert excepted_df.head() == stock_item_transformed.head()


def test_stock_item_key_range(required_dimensions_fixture):
    stock_item_transformed_stock_item_key = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types']
    ).select('StockItemKey')

    rows = []
    [rows.append(row[0]) for row in stock_item_transformed_stock_item_key.collect()]

    assert rows == [*range(0, len(rows), 1)]


def test_stock_item_coincidence_id_name(required_dimensions_fixture):
    stock_item_transformed = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types']
    )

    stock_item_transformed_dist_id = stock_item_transformed.select('WWIStockItemID', 'StockItem').distinct() \
        .withColumn("IsDistinct", F.count('WWIStockItemID').over(Window.partitionBy("StockItem")))

    assert stock_item_transformed_dist_id.agg({"IsDistinct": "max"}).collect()[0][0] < 2


def test_stock_item_valid_to_filter(required_dimensions_fixture):
    """
    Check ValidFrom ValidTo filtering
    """

    stock_item_transformed = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types']
    )
    assert 0 == stock_item_transformed.select().filter(F.col('ValidTo') <= F.col('ValidFrom')).count()


def test_stock_item_date_to(required_dimensions_fixture):
    to = datetime(year=2014, month=3, day=1)
    stock_item_transformed = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types'],
        to=to
    )

    assert stock_item_transformed.agg({"ValidFrom": "max"}).collect()[0][0] <= to


def test_empty_df(required_dimensions_fixture):
    stock_item_transformed = transform_stock_item(
        colors=required_dimensions_fixture['colors'],
        stock_groups=required_dimensions_fixture['stock_groups'],
        stock_items=required_dimensions_fixture['stock_items'],
        stock_item_stock_groups=required_dimensions_fixture['stock_item_stock_groups'],
        package_types=required_dimensions_fixture['package_types']
    )

    assert stock_item_transformed.count() > 1
