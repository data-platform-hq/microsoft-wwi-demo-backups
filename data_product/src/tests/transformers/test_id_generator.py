import datetime

import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key, zip_with_key


@pytest.fixture()
def generate_dataframe(spark_session):
    data = [(1, "James", "", "Smith", datetime.datetime(2020, 5, 17, 12)),
            (2, "Michael", "Rose", "", datetime.datetime(2020, 5, 17, 13)),
            (34, "Robert", "", "Williams", datetime.datetime(2020, 5, 17, 15)),
            (57, "Maria", "Anne", "Jones", datetime.datetime(2020, 5, 17, 13)),
            (56, "Jen", "Mary", "Brown", datetime.datetime(2020, 5, 17, 16))]

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("firstname", StringType(), True),
            StructField("middlename", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("ValidFrom", TimestampType(), True)
        ]
    )
    spark_session = SparkSession.getActiveSession()
    test_data = spark_session.createDataFrame(data=data, schema=schema)

    return test_data


def test_id_generator_key_first(generate_dataframe):
    test_data_key = zip_with_row_number_as_key(generate_dataframe, key_name='KeyID')

    assert test_data_key.columns[0] == 'KeyID'


def test_id_generator_key_last(generate_dataframe):
    test_data_key = zip_with_row_number_as_key(generate_dataframe, key_name='KeyID', key_first=False)

    assert test_data_key.columns[-1] == 'KeyID'


def test_id_generator_order_by(generate_dataframe):
    test_data_key = zip_with_row_number_as_key(generate_dataframe, key_name='KeyID', order_by='id')
    ordered_by_key = test_data_key.orderBy('KeyID').select('id').collect()
    ordered_by_value = test_data_key.orderBy('id').select('id').collect()
    assert ordered_by_value == ordered_by_key


def test_zip_with_key_key_first(generate_dataframe):
    test_data_key = zip_with_key(generate_dataframe, key_name='KeyID', key_first=True)

    assert test_data_key.columns[0] == 'KeyID'
    assert test_data_key.select('KeyID').collect() \
           == [Row(KeyID=0), Row(KeyID=1), Row(KeyID=2), Row(KeyID=3), Row(KeyID=4)]


def test_zip_with_key_key_last(generate_dataframe):
    test_data_key = zip_with_key(generate_dataframe, key_name='KeyID', key_first=False)

    assert test_data_key.columns[-1] == 'KeyID'
    assert test_data_key.select('KeyID').collect() \
           == [Row(KeyID=0), Row(KeyID=1), Row(KeyID=2), Row(KeyID=3), Row(KeyID=4)]


@pytest.mark.parametrize('offset', [1, 5, 10])
def test_zip_with_key_offset(generate_dataframe, offset):
    test_data_key = zip_with_key(generate_dataframe, key_name='KeyID', offset=offset)

    assert test_data_key.columns[0] == 'KeyID'
    assert test_data_key.select('KeyID').collect() \
           == [Row(KeyID=offset+0), Row(KeyID=offset+1), Row(KeyID=offset+2), Row(KeyID=offset+3), Row(KeyID=offset+4)]

