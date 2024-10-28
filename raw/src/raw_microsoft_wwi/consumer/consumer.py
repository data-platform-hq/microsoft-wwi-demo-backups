import datetime
import json
import os
import pyspark.sql.functions as F
from abc import abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, decode
from pyspark.sql.types import StructType, StructField, StringType, LongType
from raw_microsoft_wwi.transformer.transformer import Transformer
from raw_microsoft_wwi.writer import Writer
from raw_microsoft_wwi.configuration import StreamingConfig, IncrementalLoadConfig
from raw_microsoft_wwi.configuration.configuration import StreamingSources
from raw_microsoft_wwi.consumer.table import Table


class Consumer:
    def __init__(self, table_name: str, table_type: str, config: StreamingConfig | IncrementalLoadConfig,
                 schemas_path: str, writer: Writer, schema_generation=False) -> None:
        self.spark = SparkSession.getActiveSession()
        self.table_name = table_name
        self.config = config
        self.schemas_path = schemas_path
        self.writer = writer
        self.table_type = table_type
        self.schema_generation = schema_generation
        self.RATIO_KUT = 1000  # Ratio kafka unix timestamp
        self.RATIO_DUT = 86400  # Ratio date unix timestamp
        self.data_schema = self.set_data_schema()
        self.data_frame = self.connect()
        if not schema_generation:
            self.schema_table = self.set_schema_table()
            self.column_names = self.set_column_names()

    def set_data_schema(self):
        data_schema = StructType([
            StructField('payload', StructType([
                StructField('after', StringType(), True),
                StructField('before', StringType(), True),
                StructField('op', StringType(), True),
                StructField('source', StructType([
                    StructField('change_lsn', StringType(), True),
                    StructField('commit_lsn', StringType(), True),
                    StructField('connector', StringType(), True),
                    StructField('db', StringType(), True),
                    StructField('event_serial_no', LongType(), True),
                    StructField('name', StringType(), True),
                    StructField('schema', StringType(), True),
                    StructField('sequence', StringType(), True),
                    StructField('snapshot', StringType(), True),
                    StructField('table', StringType(), True),
                    StructField('ts_ms', LongType(), True),
                    StructField('version', StringType(), True),
                ])),
                StructField('transaction', StringType(), True),
                StructField('ts_ms', LongType(), True),
            ]
            )),
            StructField('schema', StringType(), True),
        ])

        return data_schema

    def set_schema_table(self):
        schema_file = open(
            os.path.join(
                self.schemas_path,
                f"{self.table_type}_tables_schemas",
                f"{self.table_name}.json"
            ), 'r'
        )
        return json.loads(schema_file.read())

    def set_column_names(self):
        self.schema_table.get('fields').pop()  # pop field LoadTime
        column_names = [column.get('name') for column in self.schema_table.get('fields')]
        return column_names

    def change_schema_frame(self, schema_table):
        columns_timestamp_type = []
        columns_date_type = []
        for item in schema_table['fields']:
            if item['type'] == 'timestamp':
                item['type'] = 'long'
                columns_timestamp_type.append(item['name'])
            if item['type'] == 'date':
                item['type'] = 'integer'
                columns_date_type.append(item['name'])
        schema_table = StructType.fromJson(schema_table)
        return schema_table, columns_timestamp_type, columns_date_type

    @abstractmethod
    def connect(self):
        pass

    def get_message(self):
        self.data_frame = self.data_frame \
            .select(decode('value', 'UTF-8').alias('message')) \
            .withColumn('jsonData', from_json(col('message'), self.data_schema)) \
            .select(col('jsonData.payload'), col('jsonData.payload.op'), col('jsonData.payload.source.ts_ms') \
                    .alias('EventTime')) \
            .filter(col('op').isNotNull())

    def transform_data_frame(self):
        schema_frame = self.schema_table
        schema_frame, columns_timestamp_type, columns_date_type = self.change_schema_frame(schema_frame)
        self.data_frame = self.data_frame \
            .withColumn('message', F.when(col('op') == 'd', col('payload.before')).otherwise(col('payload.after'))) \
            .withColumn('message', from_json(col('message'), schema_frame)) \
            .select('message.*', 'EventTime', 'op')

        for column_name in columns_timestamp_type:
            self.data_frame = self.data_frame\
                .withColumn(f'{column_name}', F.from_unixtime(col(f'{column_name}') / self.RATIO_KUT))

        for column_name in columns_date_type:
            self.data_frame = self.data_frame\
                .withColumn(f'{column_name}', F.from_unixtime(col(f'{column_name}') * self.RATIO_DUT))

    def disable_process_delete(self):
        self.data_frame = self.data_frame.filter(col('op') != 'd')

    def foreach_batch_function(self, df, batch_id):
        load_datetime = datetime.datetime.now()
        df_group = df.groupBy(col(df.columns[0]).alias('ID')).agg(F.max('EventTime').alias('MaxEventTime'))
        df_actual = (
            df_group.alias('df_gr')
            .join(
                df.alias('df'),
                [(F.col('df_gr.ID') == F.col(df.columns[0])) &
                 (F.col('df_gr.MaxEventTime') == F.col('EventTime'))],
                'inner'
            )
            .select(self.column_names)
        )
        # Dataframe before transformation
        df.show()

        transformed_table = Transformer.transform_table(df_actual, load_datetime)
        transformed_table.show()
        self.writer.upsert_delta_table_debezium(transformed_table, self.table_name)

    @staticmethod
    def process_row(row, schemas_path, regular_tables_list, system_versioned_tables_list):
        table = Table(json.loads(row.value)['payload'])
        table_name = f"{table.schema_name}_{table.table_name}"
        if table_name in regular_tables_list:
            table_type = 'regular_tables'
        elif table_name in system_versioned_tables_list:
            table_type = 'system_versioned_tables'
        else:
            return
        with open(os.path.join(schemas_path,f"{table_type}_schemas/{table.schema_name}_{table.table_name}.json"), 'w') as schema_file:
            schema_file.write(json.dumps(table.table_schema))

    def generate_schemas(self):
        if not self.schema_generation:
            return
        self.data_frame = self.data_frame.select(decode('value', 'UTF-8').alias('value'))
        schema_path = self.schemas_path
        regular_tables_list = self.config.regular_tables
        system_versioned_tables_list = self.config.system_versioned_tables
        os.makedirs(os.path.join(schema_path, "regular_tables_schemas"), exist_ok=True)
        os.makedirs(os.path.join(schema_path, "system_versioned_tables_schemas"), exist_ok=True)
        query = self.data_frame.writeStream.foreach(lambda x: Consumer.process_row(x, schema_path, regular_tables_list, system_versioned_tables_list)).start()
        query.awaitTermination(15)

    def consume(self):
        self.get_message()
        self.transform_data_frame()
        self.disable_process_delete()
        self.data_frame.writeStream.foreachBatch(self.foreach_batch_function).outputMode("append").start()

class KafkaConsumer(Consumer):
    def connect(self):
        df_stream = self.spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", self.config.streaming_source_url) \
            .option("subscribe", f'WWI.{self.table_name.replace("_", ".")}' if not self.schema_generation else "WWI") \
            .option("startingOffsets", "earliest") \
            .load()
        return df_stream


class EventHubConsumer(Consumer):
    def connect(self):
        properties = {}
        EVENT_HUB_NAME = f'cdc.{self.table_name.replace("_", ".")}' if not self.schema_generation else "cdc"
        connectionString = f"Endpoint={self.config.streaming_source_url}/{EVENT_HUB_NAME};EntityPath={EVENT_HUB_NAME};SharedAccessKeyName={self.config.access_key_name};SharedAccessKey={self.config.access_key}"
        startingEventPosition = {
                                "offset": -1,  
                                "seqNo": -1,            
                                "enqueuedTime": None,
                                "isInclusive": True
                            }
        
        properties['eventhubs.connectionString'] = self.spark.sparkContext._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
        properties['eventhubs.consumerGroup'] = self.config.streaming_consumer_group
        properties['eventhubs.startingPosition'] = json.dumps(startingEventPosition)
        df_stream = self.spark.readStream.format("eventhubs") \
            .options(**properties) \
            .load() \
            .withColumnRenamed("body", "value")
        
        return df_stream


class ConsumersFabric:
    @staticmethod
    def get_consumer(table_name: str, table_type: str, config: StreamingConfig | IncrementalLoadConfig,
                     schemas_path: str, writer: Writer, schema_generation=False):
        if config.streaming_source == StreamingSources.KAFKA:
            return KafkaConsumer(table_name, table_type, config, schemas_path, writer, schema_generation)

        if config.streaming_source == StreamingSources.EVENTHUB:
            return EventHubConsumer(table_name, table_type, config, schemas_path, writer, schema_generation)
