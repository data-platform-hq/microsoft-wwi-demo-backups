import datetime
from decimal import Decimal
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from data_product_microsoft_wwi.transformers.config import VALID_FROM, VALID_TO
from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_stock_item(
        colors: DataFrame,
        stock_groups: DataFrame,
        stock_items: DataFrame,
        stock_item_stock_groups: DataFrame,
        package_types: DataFrame,
        to: Optional[datetime.datetime] = None,
        offset: int = 0,
        primary_key_name: str = 'StockItemKey'
) -> DataFrame:
    if to:
        stock_groups = stock_groups.where((F.col('LoadDatetime') <= to))

    stock_group_name = (
        stock_groups.alias("sg")
        .join(
            stock_item_stock_groups.alias("sisg"),
            on=[F.col("sg.StockGroupID") == F.col("sisg.StockGroupID")],
            how="inner", )
        .withColumn('LastGroup',
                    F.max('StockItemStockGroupID')
                    .over(Window.partitionBy(['StockItemID', 'LastEditedWhen'])))
        .where(F.col('StockItemStockGroupID') == F.col('LastGroup')).drop('LastGroup'))

    join_stock_item = (
        stock_items.alias("si")
        .join(
            package_types.alias("spt"),
            on=[
                F.col("si.UnitPackageID") == F.col("spt.PackageTypeID"),
                F.col("spt.ValidFrom") <= F.col("si.ValidFrom"),
                F.col("si.ValidFrom") < F.col("spt.ValidTo")
            ],
            how="inner",
        )
        .join(
            package_types.alias("bpt"),
            on=[
                F.col("si.OuterPackageID") == F.col("bpt.PackageTypeID"),
                F.col("bpt.ValidFrom") <= F.col("si.ValidFrom"),
                F.col("si.ValidFrom") < F.col("bpt.ValidTo")
            ],
            how="inner",
        )
        .join(
            colors.alias("c"),
            on=[
                F.col("si.ColorID") == F.col("c.ColorID"),
                F.col("c.ValidFrom") <= F.col("si.ValidFrom"),
                F.col("si.ValidFrom") < F.col("c.ValidTo")
            ],
            how="left_outer",
        )
    )

    stock_item_with_stock_group_name = (
        join_stock_item
        .join(
            stock_group_name.alias("sgn"),
            on=[
                F.col("si.StockItemID") == F.col("sgn.StockItemID"),
                F.col("sgn.ValidFrom") <= F.col("si.ValidFrom"),
                F.col("si.ValidFrom") < F.col("sgn.ValidTo")
            ],
            how="left"
        )
    )

    if to:
        stock_item_with_stock_group_name = stock_item_with_stock_group_name.where((F.col('si.LoadDatetime') <= to))

    transformed_stock_item = (
        stock_item_with_stock_group_name
        .select(
            F.col("si.StockItemID").alias("WWIStockItemID"),
            F.col("si.StockItemName").alias("StockItem"),
            F.col("c.ColorName").alias("Color"),
            F.col("spt.PackageTypeName").alias("SellingPackage"),
            F.col("bpt.PackageTypeName").alias("BuyingPackage"),
            F.col("si.Brand").alias("Brand"),
            F.col("si.Size").alias("Size"),
            F.col("si.LeadTimeDays").alias("LeadTimeDays"),
            F.col("si.QuantityPerOuter").alias("QuantityPerOuter"),
            F.col("si.IsChillerStock").alias("IsChillerStock"),
            F.col("si.Barcode").alias("Barcode"),
            F.col("si.TaxRate").alias("TaxRate")
            .cast(DecimalType(18, 3)),
            F.col("si.UnitPrice").alias("UnitPrice")
            .cast(DecimalType(18, 2)),
            F.col("si.RecommendedRetailPrice").alias("RecommendedRetailPrice")
            .cast(DecimalType(18, 2)),
            F.col("si.TypicalWeightPerUnit").alias("TypicalWeightPerUnit")
            .cast(DecimalType(18, 3)),
            F.col("si.Photo").alias("Photo"),
            F.col("si.ValidFrom").alias("ValidFrom"),
            F.col("si.ValidTo").alias("ValidTo"),
            F.lit(0).alias("LineageKey")  # TODO populate lineage column
        )
        .withColumn(
            "ValidTo",
            F.lead("ValidFrom", 1, "9999-12-31 23:59:59")
            .over(Window.partitionBy("WWIStockItemID").orderBy("ValidFrom"))
        )
        .fillna("N/A", subset=["Color", "Brand", "Size", "Barcode"])
    )

    if offset == 0:
        default_df = (
            SparkSession
            .getActiveSession()
            .createDataFrame(
                [(0, 'Unknown', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 0, 0, False, 'N/A',
                  Decimal(0.000), Decimal(0.00), Decimal(0.00), Decimal(0.000),
                  None, None, None, 0)],
                transformed_stock_item.schema
            )
            .withColumn('ValidFrom', F.to_timestamp(F.lit(VALID_FROM), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .withColumn('ValidTo', F.to_timestamp(F.lit(VALID_TO), "yyyy-MM-dd HH:mm:ss.SSSSSSS"))
        )

        default_df = zip_with_row_number_as_key(default_df, key_name=primary_key_name)

        return default_df.union(zip_with_row_number_as_key(transformed_stock_item.orderBy('ValidFrom', 'WWIStockItemID'),
                                                           key_name=primary_key_name, offset=1))
    else:
        return zip_with_row_number_as_key(transformed_stock_item, key_name=primary_key_name, offset=offset)
        # NOTE for reliable id generation on really big datasets zip_with_key() from id_generator should be used
