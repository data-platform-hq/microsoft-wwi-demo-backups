from enum import Enum


class LaunchModes(Enum):
    INCREMENTAL = "incremental"
    DEBEZIUM = "debezium"


class StreamingSources(Enum):
    KAFKA = "kafka"
    EVENTHUB = "eventhubs"


class Config:
    def __init__(self, input_delta_tables_path: str, regular_tables: list, system_versioned_tables: list,
                 catalog: str, schema: str, mode: LaunchModes):
        self.input_delta_tables_path = input_delta_tables_path
        self.regular_tables = regular_tables
        self.system_versioned_tables = system_versioned_tables
        self.catalog = catalog
        self.schema = schema
        self.mode = mode
        self._check_input_delta_tables_path()
        self._check_databricks_schema()

    def _check_input_delta_tables_path(self):
        if not self.input_delta_tables_path:
            raise ValueError("<input_delta_tables_path> cannot be empty or None!")

    def _check_databricks_schema(self):
        if not self.schema:
            raise ValueError("<databricks_schema> cannot be empty or None!")


class StreamingConfig(Config):
    def __init__(self, input_delta_tables_path: str, regular_tables: list,
                 system_versioned_tables: list, catalog: str, schema: str,
                 streaming_source: StreamingSources, streaming_source_url: str,
                 eventhubs_access_key_name: str = None, eventhubs_access_key: str = None,
                 streaming_consumer_group: str = None):
        super().__init__(input_delta_tables_path=input_delta_tables_path, regular_tables=regular_tables,
                         system_versioned_tables=system_versioned_tables, catalog=catalog,
                         schema=schema, mode=LaunchModes.DEBEZIUM)
        self.streaming_source = streaming_source
        self.streaming_source_url = streaming_source_url
        self.streaming_consumer_group = streaming_consumer_group
        if self.streaming_source == StreamingSources.EVENTHUB:
            self.access_key_name = eventhubs_access_key_name
            self.access_key = eventhubs_access_key
            self._check_eventhubs_keys()

    def _check_streaming_source_url(self):
        if not self.streaming_source_url:
            raise ValueError("<streaming_source_url> cannot be empty or None!")

    def _check_eventhubs_keys(self):
        if not self.access_key or not self.access_key_name:
            raise ValueError("<access_key_name> and <access_key> cannot be empty or None if Azure Eventhubs is used as source!")


class IncrementalLoadConfig(Config):
    def __init__(self, input_delta_tables_path: str, regular_tables: list,
                 system_versioned_tables: list, catalog: str, schema: str,
                 source_database_jdbc_url: str, e2e_expected_data_delta_path: str, run_mode: str):
        super().__init__(input_delta_tables_path=input_delta_tables_path, regular_tables=regular_tables,
                         system_versioned_tables=system_versioned_tables, catalog=catalog,
                         schema=schema, mode=LaunchModes.INCREMENTAL)
        self.source_database_jdbc_url = source_database_jdbc_url
        self.e2e_expected_data_delta_path = e2e_expected_data_delta_path
        self.run_mode = run_mode
        self._check_source_database_jdbc_url()

    def _check_source_database_jdbc_url(self):
        if not self.source_database_jdbc_url:
            raise ValueError("<source_database_jdbc_url> cannot be empty or None!")
