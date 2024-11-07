import datetime
from typing import Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_city(cities: DataFrame,
                   countries: DataFrame,
                   state_provinces: DataFrame,
                   to: Optional[datetime.datetime] = None,
                   offset: int = 0,
                   primary_key_name: str = 'CityKey') -> DataFrame:
    if to:
        cities = cities.where((F.col('LoadDatetime') <= to))
        state_provinces = state_provinces.where((F.col('LoadDatetime') <= to))
        countries = countries.where((F.col('LoadDatetime') <= to))


    cities_staging = (
        cities
        .join(
            state_provinces.alias('sp'),
            [cities.StateProvinceID == F.col('sp.StateProvinceID')],
            'inner'
        )
        .join(
            countries.alias('c'),
            [F.col('c.CountryID') == F.col('sp.CountryID')],
            'inner'
        )
        .withColumn('RecordValidFrom', F.greatest(cities.ValidFrom, F.col('sp.ValidFrom'), F.col('c.ValidFrom')))
        .withColumn('RecordValidTo', F.least(cities.ValidTo, F.col('sp.ValidTo'), F.col('c.ValidTo')))
        .filter(F.col('RecordValidTo') > F.col('RecordValidFrom'))
        .withColumn('LineageKey', F.lit(1))
        .drop_duplicates(['CityID', 'ValidFrom'])
        .select(
            cities.CityID.alias('WWICityID'),
            cities.CityName.alias('City'),
            state_provinces.StateProvinceName.alias('StateProvince'),
            countries.CountryName.alias('Country'),
            countries.Continent,
            state_provinces.SalesTerritory,
            countries.Region,
            countries.Subregion,
            cities.Location,
            cities.LatestRecordedPopulation,
            F.col('RecordValidFrom').alias('ValidFrom'),
            F.col('RecordValidTo').alias('ValidTo'),
            F.col('LineageKey')
        )
        .fillna({'LatestRecordedPopulation': 0})
    )

    cities_staging = cities_staging.withColumn("LocationCopy",
                                               F.regexp_replace(cities_staging["Location"], "[\()]", ""))

    cities_staging = (
        cities_staging
        .withColumn("EndOfTime", F.to_timestamp(F.lit(VALID_TO)))
        .withColumn("DataProductLoadDatetime", F.to_timestamp(F.lit(to)))
        .withColumn("Longitude", F.split(cities_staging["LocationCopy"], " ").getItem(1)
                                  .cast(DoubleType()))
        .withColumn("Latitude", F.split(cities_staging["LocationCopy"], " ").getItem(2)
                    .cast(DoubleType()))
        .withColumn("WWICityIDGroupRowNumber", F.row_number().over(Window.partitionBy("WWICityID").orderBy(F.col("ValidFrom").desc())))
        .withColumn("ValidTo", (F.when((F.col("WWICityIDGroupRowNumber") == 1) & (F.col("ValidTo") > F.col("DataProductLoadDatetime")), F.col("EndOfTime"))
                                .when((F.col("WWICityIDGroupRowNumber") == 1) & (F.col("ValidTo") < F.col("DataProductLoadDatetime")), F.col("ValidTo"))
                                .when((F.col("WWICityIDGroupRowNumber") != 1), F.col("ValidTo"))
                                ))
    )

    cities_staging = (
        cities_staging
        .drop(cities_staging['LocationCopy'])
        .drop(cities_staging['EndOfTime'])
        .drop(cities_staging['DataProductLoadDatetime'])
        .drop(cities_staging['WWICityIDGroupRowNumber'])
    )


    if offset == 0:
        default_df = (
            SparkSession
            .getActiveSession()
            .createDataFrame(
                [(0, 'Unknown', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', None, 0, None, None, 0, 0.0, 0.0)],
                cities_staging.schema
            )
            .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
        )

        default_df = zip_with_row_number_as_key(default_df, key_name=primary_key_name)
        return default_df.union(zip_with_row_number_as_key(cities_staging.orderBy('ValidFrom', 'WWICityID'),
                                                           key_name=primary_key_name, offset=1))
    else:
        return zip_with_row_number_as_key(cities_staging, key_name=primary_key_name, offset=offset)
