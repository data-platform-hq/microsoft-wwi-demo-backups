from data_product_microsoft_wwi.configuration import OutputFactTableName, OutputDimensionsTableNames, \
    output_table_name_path
from pyspark.sql import SparkSession


def assert_table(spark, delta_tables_expected_full, delta_tables_actual_full):
    actual_data = load_delta_table(spark=spark, path=delta_tables_actual_full)

    expected_data = load_delta_table(spark=spark, path=delta_tables_expected_full)

    return (
            actual_data.exceptAll(expected_data).count() == 0 and
            expected_data.exceptAll(actual_data).count() == 0
           )


def assert_dimensions(spark, delta_tables_expected_path, delta_tables_actual_path):
    for table_name in OutputDimensionsTableNames:
        if not assert_table(spark=spark,
                            delta_tables_expected_full=(f'{delta_tables_expected_path}/Dimension/'
                                                        f'{table_name.value}'),
                            delta_tables_actual_full=f'{delta_tables_actual_path}/Dimension/'
                                                     f'{table_name.value}'):
            raise Exception(f"Actual and expected data isn't same. E2E test failed."
                            f"Table {table_name} is not equal to expected table")


def assert_facts(spark, delta_tables_expected_path, delta_tables_actual_path):
    for table_name in OutputFactTableName:
        if not assert_table(spark=spark,
                            delta_tables_expected_full=f'{delta_tables_expected_path}/Fact/'
                                                       f'{table_name.value}',
                            delta_tables_actual_full=f'{delta_tables_actual_path}/Fact/'
                                                     f'{table_name.value}'):
            raise Exception(f"Actual and expected data isn't same. E2E test failed."
                            f"Table {table_name} is not equal to expected table")


def run_e2e_workflow(delta_tables_expected_path, delta_tables_actual_path):
    print("Starting compare actual tables with expected")

    spark = SparkSession.getActiveSession()

    assert_dimensions(spark, delta_tables_expected_path, delta_tables_actual_path)
    assert_facts(spark, delta_tables_expected_path, delta_tables_actual_path)

    print("E2E test is finished. All actual delta tables are equal to expected")


def load_delta_table(spark, path):
    return (spark
            .read
            .format("delta")
            .load(path)
            )
