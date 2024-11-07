import logging
import json
import requests
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def local_spark_session_init(py4j_log_level: str = None, spark_log_level: str = None) -> SparkSession:
    """Create spark session and apply configurations"""
    scala_version = "2.12"

    if py4j_log_level:
        logger = logging.getLogger('py4j')
        logger.setLevel(py4j_log_level)

    spark_session = configure_spark_with_delta_pip(
        SparkSession.builder
        .master('local[*]')
        .appName('IaCDA')
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
        ["com.microsoft.sqlserver:mssql-jdbc:8.4.0.jre11",
         f"com.microsoft.azure:spark-mssql-connector_{scala_version}:1.2.0",
         "org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.2",
         "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2",
         "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22"
        ]
    ).getOrCreate()

    if spark_log_level:
        logger = logging.getLogger('py4j')
        logger.setLevel(spark_log_level)
        spark_session.sparkContext.setLogLevel(spark_log_level)

    return spark_session


def create_debezium_connector(mssql_port, mssql_db_name, mssql_user_name, mssql_password, kafka_url, kafka_port, kafka_connect_port, kafka_connect_url,
                              schemas_list, tables_list, connector_name = "epmadpafic_raw_connector"):

        kafka_connection_string = "kafka:9092" if kafka_url == 'localhost' else f"{kafka_url}:{kafka_port}" #parametrization for non local kafka
        configuration = {
            "name" : connector_name,
            "config" : {
                "connector.class" :  "io.debezium.connector.sqlserver.SqlServerConnector",
                "database.hostname" : "epmadpafic",
                "database.port" : mssql_port,
                "database.user" : mssql_user_name,
                "database.password" : mssql_password,
                "database.dbname": mssql_db_name,
                "database.server.name": "WWI",
                "schema.include.list" : schemas_list,
                "table.include.list": tables_list,
                "database.history.kafka.bootstrap.servers" : kafka_connection_string,
                "database.history.kafka.topic": "dbhistory.WWI",
                "decimal.handling.mode" : "double",
                "time.precision.mode": "connect"
            }
        }
        resp = requests.get(url=f'http://{kafka_connect_url}:{kafka_connect_port}/connectors/{connector_name}').status_code
        if resp == 404:
            request_result = requests.post(url=f'http://{kafka_connect_url}:{kafka_connect_port}/connectors/', data=json.dumps(configuration),
                            headers={"Accept": "application/json", "Content-Type": "application/json"})
        else:
            request_result = requests.put(url=f'http://{kafka_connect_url}:{kafka_connect_port}/connectors/{connector_name}/config',
                                            data=json.dumps(configuration["config"]),
                                            headers={"Accept": "application/json", "Content-Type": "application/json"})
        print(request_result.text)


def get_debezium_settings(tables):

    tables_list = ''
    schemas_set = set()
    black_list = 'Warehouse_ColdRoomTemperatures'

    for item in tables:
        if item in black_list:
            continue
        schemas_set.add(item.split('_')[0])
        tables_list += item.replace('_', '.') + ", "

    schemas_list = ', '.join(item for item in schemas_set)
    return tables_list, schemas_list
