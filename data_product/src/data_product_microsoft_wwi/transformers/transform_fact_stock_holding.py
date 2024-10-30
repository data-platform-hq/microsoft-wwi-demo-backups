import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_fact_stock_holding(stock_item_holdings: DataFrame,
                                 dimension_stock_item: DataFrame,
                                 since: Optional[datetime.datetime] = None,
                                 to: Optional[datetime.datetime] = None,
                                 offset: int = 0,
                                 primary_key_name: str = 'StockHoldingKey') -> DataFrame:
    if since and to:
        stock_item_holdings = stock_item_holdings.where((col('LoadDatetime') > since) & (col('LoadDatetime') <= to))

    stock_holding_staging = stock_item_holdings.alias('sih') \
                            .select(col('sih.QuantityOnHand').alias('QuantityOnHand'),
                                    col('sih.BinLocation').alias('BinLocation'),
                                    col('sih.LastStocktakeQuantity').alias('LastStocktakeQuantity'),
                                    col('sih.LastCostPrice').alias('LastCostPrice'),
                                    col('sih.ReorderLevel').alias('ReorderLevel'),
                                    col('sih.TargetStockLevel').alias('TargetStockLevel'),
                                    col('sih.StockItemID').alias('WWIStockItemID'),
                                    col('sih.LoadDateTime').alias('LoadDateTime'),
                                    col('sih.LoadDatetime').alias("RawDataLoadDatetime")) \
                            .withColumn('LineageKey', lit(12)) \
                            .orderBy(col('WWIStockItemID'))

    fact_stock_holding = stock_holding_staging.alias('s') \
        .join(dimension_stock_item.alias('si'),
              [col('s.WWIStockItemID') == col('si.WWIStockItemID')],
              how='left') \
        .coalesce(1) \
        .orderBy('ValidTo', ascending=False) \
        .dropDuplicates(['WWIStockItemID']) \
        .selectExpr(
            "COALESCE(si.StockItemKey, 0) AS StockItemKey",
            "s.QuantityOnHand",
            "s.BinLocation",
            "s.LastStocktakeQuantity",
            "s.LastCostPrice",
            "s.ReorderLevel",
            "s.TargetStockLevel",
            "s.LineageKey",
            "s.RawDataLoadDatetime"
        )

    return zip_with_row_number_as_key(fact_stock_holding, key_name=primary_key_name, offset=offset,
                                      order_by='StockItemKey')
    # NOTE for reliable id generation on really big datasets zip_with_key() from id_generator should be used
