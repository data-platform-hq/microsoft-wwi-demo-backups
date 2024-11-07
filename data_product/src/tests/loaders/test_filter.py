import arrow
import pytest
from pyspark.sql.utils import AnalysisException

from data_product_microsoft_wwi.configuration import OutputFactTableName, \
    output_transformed_tables_primary_keys_names
from data_product_microsoft_wwi.loaders.filter import DataFilter
from tests.conftest import DEST_SCHEMAS, fixtures_path
from tests.helpers import CSVReader


@pytest.fixture(scope="function")
def required_dataframe_fixture(request):
    reader = CSVReader()
    dimensions_response = {}

    for dimension in request.param:
        dimensions_response[dimension[0]] = reader.get_data_frame(f"{fixtures_path}/{dimension[1]}", dimension[2])

    return dimensions_response


@pytest.mark.parametrize("sales_fact_transformed_params", [(34, 'filter/fact_sale_trim.csv'),
                                                           (35, 'filter/fact_sale_trim_not_in_sequence.csv')])
def test_filter_offset(spark_session, mocker, sales_fact_transformed_params: tuple):
    reader_csv = CSVReader()
    fact_sale_df = reader_csv.get_data_frame(f"{fixtures_path}/{sales_fact_transformed_params[1]}",
                                             DEST_SCHEMAS['Sale'])

    reader_delta_mock = mocker.MagicMock()
    reader_delta_mock.get_transformed_df = mocker.Mock(return_value=fact_sale_df)

    offset = DataFilter().get_offset(reader_delta_mock, OutputFactTableName.SALE,
                                     output_transformed_tables_primary_keys_names)

    assert offset == sales_fact_transformed_params[0] + 1


def test_filter_offset_error(mocker):
    """
    Find offset if delta table has not been created yet
    """
    reader_delta_mock = mocker.MagicMock()
    reader_delta_mock.get_transformed_df = mocker.Mock(side_effect=AnalysisException('foo', None))

    offset = DataFilter().get_offset(reader_delta_mock, OutputFactTableName.SALE,
                                     output_transformed_tables_primary_keys_names)

    assert offset == 0


@pytest.mark.parametrize("required_dataframe_fixture, expected",
                         [((('sales', 'filter/fact_sale_trim.csv', DEST_SCHEMAS['Sale']),), '2022-07-25')],
                         indirect=["required_dataframe_fixture"])
def test_get_since_date(mocker, required_dataframe_fixture, expected):
    data_frame_filter = DataFilter()
    reader_delta_mock = mocker.MagicMock()
    reader_delta_mock.get_transformed_df = mocker.Mock(return_value=required_dataframe_fixture['sales'])

    date_time_since = data_frame_filter.get_since_date(reader_delta_mock, OutputFactTableName.SALE,
                                                       'RawDataLoadDatetime')

    assert arrow.get(expected).format('YYYY-MM-DD') == arrow.get(date_time_since).format('YYYY-MM-DD')
