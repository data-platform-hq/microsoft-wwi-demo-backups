from datetime import datetime

import arrow
import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers import transform_employee
from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from tests.conftest import DEST_SCHEMAS, SRS_SCHEMAS, fixtures_path
from tests.helpers import CSVReader

HEAD_ROW_STRUCTURE = (0, 0, 'Unknown', 'N/A', False, None, None, None, 0)


@pytest.fixture(params=[
    (['people', 'Application/People', SRS_SCHEMAS['People']],)
])
def required_dimensions_fixture(spark_session, request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


def test_check_schema(required_dimensions_fixture):
    employee = transform_employee(required_dimensions_fixture['people'])

    assert DEST_SCHEMAS['Employee'] == employee.schema


def test_check_dataframe_structure(required_dimensions_fixture):
    spark_session = SparkSession.getActiveSession()

    people_transformed = transform_employee(required_dimensions_fixture['people'])

    excepted_df = (spark_session.createDataFrame([HEAD_ROW_STRUCTURE], DEST_SCHEMAS['Employee'])
                   .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                   .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS")))

    assert excepted_df.head() == people_transformed.head()


def test_check_lineage_key(required_dimensions_fixture):
    people_df = required_dimensions_fixture['people']
    people_transformed = transform_employee(people_df)
    amount_people_transformed = people_transformed.count()

    filtered_employees_before_transform = people_df.where(people_df.IsEmployee == 1).count()

    assert 0 == (amount_people_transformed - filtered_employees_before_transform)
    assert 1 == people_transformed.where(people_transformed.LineageKey == 0).count()


def test_employee_key_range(required_dimensions_fixture):
    people_transformed_employee_key_df = transform_employee(required_dimensions_fixture['people']).select('EmployeeKey')

    rows = []
    [rows.append(row[0]) for row in people_transformed_employee_key_df.collect()]

    assert rows == [*range(0, len(rows), 1)]


def test_employee_coincidence_id_name(required_dimensions_fixture):
    people_transformed = transform_employee(required_dimensions_fixture['people'])

    people_transformed_dist_id = people_transformed.select('WWIEmployeeID', 'Employee').distinct() \
        .withColumn("IsDistinct", F.count('WWIEmployeeID').over(Window.partitionBy("Employee")))

    assert people_transformed_dist_id.agg({"IsDistinct": "max"}).collect()[0][0] < 2


def test_employee_valid_to_filter(required_dimensions_fixture):
    """
    Check ValidFrom ValidTo filtering
    """

    people_transformed = transform_employee(required_dimensions_fixture['people'])

    assert 0 == people_transformed.select().filter(F.col('ValidTo') <= F.col('ValidFrom')).count()


def test_employee_date_to(required_dimensions_fixture):
    to = datetime(year=2014, month=3, day=1)
    result = transform_employee(required_dimensions_fixture['people'], to=to)

    assert result.agg({"ValidFrom": "max"}).collect()[0][0] <= to


def test_empty_df(required_dimensions_fixture):
    people_transformed = transform_employee(required_dimensions_fixture['people'])

    assert people_transformed.count() > 1


def test_employee_with_the_same_valid_from_valid_to(required_dimensions_fixture):
    people = transform_employee(required_dimensions_fixture['people'])
    employee_with_the_same_valid_from_valid_to = people.filter(F.col("ValidFrom") == F.col("ValidTo")).count()

    assert employee_with_the_same_valid_from_valid_to == 0


def test_employee_inc_loading_if_date_to_less_available_data(required_dimensions_fixture):
    employee = transform_employee(required_dimensions_fixture['people'], arrow.get('2012-12-31').datetime)
    assert DEST_SCHEMAS['Employee'] == employee.schema
    assert 0 == employee.count()

    employee = transform_employee(required_dimensions_fixture['people'], arrow.get('2014-12-31').datetime)
    assert 3 == employee.count()
