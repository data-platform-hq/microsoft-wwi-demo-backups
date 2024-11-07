import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_payment_method(
        payment_method: DataFrame,
        to: Optional[datetime.datetime] = None,
        offset: int = 0,
        primary_key_name: str = 'PaymentMethodKey'
) -> DataFrame:
    if to:
        payment_method = payment_method.where((F.col('LoadDatetime') <= to))

    payment_method_staging = (
        payment_method
        .withColumn("LineageKey", F.lit(4))
        .select(
            payment_method.PaymentMethodID.alias('WWIPaymentMethodID'),
            payment_method.PaymentMethodName.alias('PaymentMethod'),
            payment_method.ValidFrom,
            payment_method.ValidTo,
            F.col('LineageKey')
        )
    )

    if offset == 0:
        default_df = (
            SparkSession
            .getActiveSession()
            .createDataFrame(
                [(0, 'Unknown', None, None, 0)],
                payment_method_staging.schema
            )
            .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .withColumn('LineageKey', F.lit(0))
        )

        payment_method_staging = default_df.union(payment_method_staging).orderBy('ValidFrom', 'WWIPaymentMethodID',
                                                                                  ascending=True)

    return zip_with_row_number_as_key(payment_method_staging, key_name=primary_key_name, offset=offset)
    # NOTE for reliable id generation on really big datasets zip_with_key() from id_generator should be used
