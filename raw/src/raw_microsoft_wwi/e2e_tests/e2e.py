from pyspark.sql import SparkSession
from raw_microsoft_wwi.configuration import Config


def run_e2e_workflow(config: Config):
    spark = SparkSession.getActiveSession()

    for table_name in config.regular_tables:
        if not assert_delta_table(table_name, config, spark):
            raise Exception("Actual and expected data isn't same. E2E test failed")

    for table_name in config.system_versioned_tables:
        if not assert_delta_table(table_name, config, spark):
            raise Exception("Actual and expected data isn't same. E2E test failed")

    print("E2E test is passed successfully")


def assert_delta_table(table_name, config, spark):
    actual_data = (spark
                   .read
                   .format("delta")
                   .load(f'{config.input_delta_tables_path}/{table_name.replace("_", "/")}')
                   )

    expected_data = (spark
                     .read
                     .format("delta")
                     .load(f'{config.e2e_expected_data_delta_path}/{table_name.replace("_", "/")}')
                     )

    return (
           actual_data.exceptAll(expected_data).count() == 0 and 
           expected_data.exceptAll(actual_data).count() == 0
           )
            
