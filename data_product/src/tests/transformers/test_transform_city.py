from datetime import datetime

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers import transform_city
from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from tests.conftest import DEST_SCHEMAS, SRS_SCHEMAS, fixtures_path
from tests.helpers import CSVReader

HEAD_ROW_STRUCTURE = (0, 0, 'Unknown', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', None, 0, None, None, 0, 0.0, 0.0)


@pytest.fixture(params=[
    (['cities', 'Application/Cities', SRS_SCHEMAS['Cities']],
     ['countries', 'Application/Countries', SRS_SCHEMAS['Countries']],
     ['state_provinces', 'Application/StateProvinces', SRS_SCHEMAS['StateProvinces']])
])
def required_dimensions_fixture(spark_session, request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


def test_check_schema(required_dimensions_fixture):
    city = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces']
    )

    assert DEST_SCHEMAS['City'] == city.schema


def test_check_first_row_if_offset_is_0(required_dimensions_fixture):
    spark_session = SparkSession.getActiveSession()

    cities_df = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces']
    )

    excepted_df = (spark_session.createDataFrame([HEAD_ROW_STRUCTURE], DEST_SCHEMAS['City'])
                   .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                   .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS")))

    assert excepted_df.head() == cities_df.head()


def test_cities_key_range(required_dimensions_fixture):
    cities_transformed_city_key = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces']
    ).select('CityKey')

    rows = []
    [rows.append(row[0]) for row in cities_transformed_city_key.collect()]

    assert rows == [*range(0, len(rows), 1)]


def test_cities_coincidence_id_name(required_dimensions_fixture):
    cities_transformed = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces']
    )

    cities_transformed_dist_id = cities_transformed.select('WWICityID', 'City', 'StateProvince').distinct() \
        .withColumn("IsDistinct", F.count('WWICityID').over(Window.partitionBy("City", "StateProvince")))

    assert cities_transformed_dist_id.agg({"IsDistinct": "max"}).collect()[0][0] < 2


def test_city_valid_to_filter(required_dimensions_fixture):
    """
    Check ValidFrom ValidTo filtering
    """
    transformed_city = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces']
    )

    assert 0 == transformed_city.select().filter(F.col('ValidTo') <= F.col('ValidFrom')).count()


def test_city_date_to(required_dimensions_fixture):
    to = datetime(year=2014, month=3, day=1)
    cities_transformed = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces'],
        to=to
    )

    assert cities_transformed.agg({"ValidFrom": "max"}).collect()[0][0] <= to


def test_empty_df(required_dimensions_fixture):
    cities_transformed = transform_city(
        cities=required_dimensions_fixture['cities'],
        countries=required_dimensions_fixture['countries'],
        state_provinces=required_dimensions_fixture['state_provinces']
    )

    assert cities_transformed.count() > 1
