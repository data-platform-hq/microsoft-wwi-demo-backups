import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import LongType, IntegerType


def zip_with_key(df: DataFrame, key_name: str = 'Key', offset: int = 0, key_first: bool = True):
    """
    Generate Key column to input DataFrame without converting to RDD and shuffling to one partition bottlenecks.

    Parameters
    ----------
    df: DataFrame
        Input sorted DataFrame to add Key column to.
    key_name : str, default 'Key'
        Name of the column with key.
    offset : int, default 1
        Starting value of identifier.
    key_first: bool, default True
        The position of the key column to be added.
    Returns
    -------
    DataFrame
        Input DataFrame with key column added.
    """
    df_with_partition_id = (
        df
        .withColumn('PartitionId', F.spark_partition_id())
        .withColumn('MonotonicId', F.monotonically_increasing_id())
        .cache()
    )

    partitions_offsets = (
        df_with_partition_id
        .groupBy('PartitionId')
        .agg(
            F.count(F.lit(1)).alias('PartitionCount'),
            F.first('MonotonicId').alias('MonotonicId')
        )
        .orderBy('PartitionId')
        .select(
            F.col('PartitionId'),
            (F.sum('PartitionCount')
             .over(Window.orderBy('PartitionId').partitionBy(F.lit(1))) - F.col('PartitionCount')
             - F.col('MonotonicId') + F.lit(offset))
            .alias('PartitionOffset')
        )
        .collect()
    )

    partitions_offsets = {row['PartitionId']: row['PartitionOffset'] for row in partitions_offsets}

    map_partition_id_to_offset = F.udf(lambda partition_id: partitions_offsets[partition_id], LongType())

    df_with_key = (
        df_with_partition_id
        .withColumn('PartitionOffset', map_partition_id_to_offset(F.col('PartitionId')))
        .withColumn(key_name, F.col('PartitionOffset') + F.col('MonotonicId'))
        .drop('PartitionId', 'MonotonicId', 'PartitionOffset')
        .withColumn(key_name, F.col(key_name).cast(IntegerType()))
    )

    if key_first:
        df_with_key = df_with_key.select(key_name, *df_with_key.columns[:-1])

    return df_with_key


def zip_with_row_number_as_key(df: DataFrame, key_name: str = 'Key', offset: int = 0,
                               key_first: bool = True, order_by: str = 'ValidFrom'):
    """
    Generate Key column based on row number. This causes moving all the data to one partition.

    Parameters
    ----------
    df: DataFrame
        Input sorted DataFrame to add Key column to.
    key_name : str, default 'Key'
        Name of the column with key.
    offset : int, default 0
        Starting value of identifier.
    key_first: bool, default True
        Whether to put key column to be the first.
    order_by: str, default 'ValidFrom'
        Column name to sort by when applying row_number as key.
    Returns
    -------
    DataFrame
        Input DataFrame with key column added.
    """
    df_with_key = df.withColumn(
        key_name,
        F.row_number().over(Window.orderBy(order_by).partitionBy(F.lit(1))) + F.lit(offset - 1)
    )

    if key_first:
        df_with_key = df_with_key.select(key_name, *df_with_key.columns[:-1])

    return df_with_key
