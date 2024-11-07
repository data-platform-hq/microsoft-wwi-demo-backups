import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_supplier(suppliers: DataFrame,
                       supplier_categories: DataFrame,
                       people: DataFrame,
                       to: Optional[datetime.datetime] = None,
                       offset: int = 0,
                       primary_key_name: str = 'SupplierKey') -> DataFrame:
    if to:
        suppliers = suppliers.where((F.col('LoadDatetime') <= to))
        supplier_categories = supplier_categories.where((F.col('LoadDatetime') <= to))
        people = people.where((F.col('LoadDatetime') <= to))

    supplier_staging = (
        suppliers.alias('s')
        .join(
            supplier_categories.alias('sc'),
            [(F.col('s.SupplierCategoryID') == F.col('sc.SupplierCategoryID')) &
             ((F.col('s.ValidFrom') <= F.col('sc.ValidTo')) & (F.col('sc.ValidFrom') <= F.col('s.ValidTo')))],
            'inner'
        )
        .join(
            people.alias('p'),
            [(F.col('s.PrimaryContactPersonID') == F.col('p.PersonID')) &
             ((F.col('s.ValidFrom') <= F.col('p.ValidTo')) & (F.col('p.ValidFrom') <= F.col('s.ValidTo')))],
            'inner'
        )
        .withColumn("LineageKey", F.lit(6))
        .select(
            F.col('s.SupplierID').alias('WWISupplierID'),
            F.col('s.SupplierName').alias('Supplier'),
            F.col('sc.SupplierCategoryName').alias('Category'),
            F.col('p.FullName').alias('PrimaryContact'),
            F.col('s.SupplierReference').alias('SupplierReference'),
            F.col('s.PaymentDays').alias('PaymentDays'),
            F.col('s.DeliveryPostalCode').alias('PostalCode'),
            F.col('s.ValidFrom').alias('ValidFrom'),
            F.col('s.ValidTo').alias('ValidTo'),
            F.col('LineageKey')
        )
    )
    supplier_staging = supplier_staging.drop_duplicates()

    if offset == 0:
        default_df = (
            SparkSession
            .getActiveSession()
            .createDataFrame(
                [(0, 'Unknown', 'N/A', 'N/A', 'N/A', 0, 'N/A', None, None, 0)],
                supplier_staging.schema
            )
            .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .withColumn('LineageKey', F.lit(0))
        )

        supplier_staging = default_df.union(supplier_staging).orderBy('WWISupplierID', ascending=True)

    return zip_with_row_number_as_key(supplier_staging, key_name=primary_key_name, offset=offset)
