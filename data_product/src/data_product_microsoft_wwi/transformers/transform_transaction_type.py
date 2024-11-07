import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_transaction_type(transactions_types: DataFrame,
                              to: Optional[datetime.datetime] = None,
                              offset: int = 0,
                              primary_key_name: str = 'TransactionTypeKey') -> DataFrame:


    if to:
        transactions_types = transactions_types.where((F.col('LoadDatetime') <= to))
    transactions_types_staging = (
        transactions_types
        .withColumn("LineageKey", F.lit(7))
        .drop_duplicates(['TransactionTypeID', 'ValidFrom'])
        .select(
                transactions_types.TransactionTypeID.alias('WWITransactionTypeID'),
                transactions_types.TransactionTypeName.alias('TransactionType'),
                transactions_types.ValidFrom,
                transactions_types.ValidTo,
                F.col('LineageKey'),
            )
        )
    if offset == 0:
        default_df = (
            SparkSession.getActiveSession()
                        .createDataFrame([(0, 'Unknown', None, None, 0)],
                                         schema=transactions_types_staging.schema
                        )
                        .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                        .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                        .withColumn('LineageKey', F.lit(0))
        )
        transactions_types_staging = default_df.union(transactions_types_staging).orderBy('ValidFrom', 'WWITransactionTypeID')
    return zip_with_row_number_as_key(transactions_types_staging, key_name = primary_key_name, offset=offset)    