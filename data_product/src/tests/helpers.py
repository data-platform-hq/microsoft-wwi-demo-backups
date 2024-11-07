from pyspark.sql import DataFrame

from data_product_microsoft_wwi.loaders.common import SessionInit
from tests.conftest import SRS_SCHEMAS
from tests.conftest import fixtures_path


def tsv_loader(table_path, schema_name, spark_session_init):
    return (spark_session_init.read.format("csv")
            .option('header', True)
            .option('sep', '\t')
            .load(f'{fixtures_path}/{table_path}', schema=SRS_SCHEMAS.get(schema_name)))


class CSVReader(SessionInit):
    """
    Read CSV and hold DataFrame
    """

    def __init__(self):
        self._data_frames = {}

    def get_data_frame(self, table_path: str, schema) -> DataFrame:
        if self._data_frames.get(table_path):
            return self._data_frames.get(table_path)

        spark_session = self.get_spark_session()

        self._data_frames[table_path] = (spark_session.read.format("csv")
                                         .option('header', True)
                                         .option('sep', '\t')
                                         .load(table_path, schema=schema))

        return self._data_frames[table_path]
