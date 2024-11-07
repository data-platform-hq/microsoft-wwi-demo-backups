import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from data_product_microsoft_wwi.transformers.config import VALID_TO, VALID_FROM
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_employee(people: DataFrame,
                       to: Optional[datetime.datetime] = None,
                       offset: int = 0,
                       primary_key_name: str = 'EmployeeKey') -> DataFrame:
    if to:
        people = people.where((F.col('LoadDatetime') <= to))

    employees_staging = (
        people
        .where(people.IsEmployee == 1)
        .where(F.col("ValidFrom") != F.col("ValidTo"))
        .withColumn("LineageKey", F.lit(3))
        .drop_duplicates(['PersonID', 'ValidFrom'])
        .select(
            people.PersonID.alias('WWIEmployeeID'),
            people.FullName.alias('Employee'),
            people.PreferredName,
            people.IsSalesperson,
            people.Photo,
            people.ValidFrom,
            people.ValidTo,
            F.col('LineageKey')
        )
        .withColumn("ValidTo", (F.when(F.col("ValidTo") < F.lit(to), F.to_timestamp(F.col("ValidTo")))
                                .otherwise(F.to_timestamp(F.lit(VALID_TO)))))
    )

    if offset == 0:
        employees_staging_with_pk = zip_with_row_number_as_key(employees_staging.orderBy('ValidFrom', 'WWIEmployeeID'),
                                                               key_name=primary_key_name, offset=1)
        if 0 != employees_staging.count():
            default_df = (
                SparkSession
                .getActiveSession()
                .createDataFrame(
                    [(0, 'Unknown', 'N/A', False, None, None, None, 0)],
                    employees_staging.schema
                )
                .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
                .withColumn('LineageKey', F.lit(0))
            )

            default_df = zip_with_row_number_as_key(default_df, key_name=primary_key_name)

            return default_df.union(employees_staging_with_pk)

        else:
            return employees_staging_with_pk

    else:
        return zip_with_row_number_as_key(employees_staging, key_name=primary_key_name, offset=offset)
        # NOTE for reliable id generation on really big datasets zip_with_key() from id_generator should be used
