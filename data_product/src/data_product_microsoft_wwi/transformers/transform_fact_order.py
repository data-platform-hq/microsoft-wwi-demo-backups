import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as pysp_round

from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_fact_order(sales_orders: DataFrame,
                         sales_orderlines: DataFrame,
                         warehouse_package_types: DataFrame,
                         sales_customers: DataFrame,
                         dimension_city: DataFrame,
                         dimension_customer: DataFrame,
                         dimension_stock_item: DataFrame,
                         dimension_employee: DataFrame,
                         since: Optional[datetime.datetime] = None,
                         to: Optional[datetime.datetime] = None,
                         offset: int = 0,
                         primary_key_name: str = 'OrderKey') -> DataFrame:
    if since and to:
        sales_orders = sales_orders.where((col('LoadDatetime') > since) & (col('LoadDatetime') <= to))

    order_staging = (
        sales_orders.alias('so')
        .join(
            sales_orderlines.alias("sol"),
            col("so.OrderID") == col("sol.OrderID"),
            "inner"
        )
        .join(
            sales_customers.alias("sc"),
            col("so.CustomerID") == col("sc.CustomerID"),
            "inner"
        )
        .join(
            warehouse_package_types.alias("wpt"),
            col("sol.PackageTypeID") == col("wpt.PackageTypeID"),
            "inner"
        )
        .withColumn('LineageKey', lit(9))
        .select(
            col('so.OrderDate').cast('date').alias('OrderDateKey'),
            col('sol.PickingCompletedWhen').cast('date').alias('PickedDateKey'),
            col('so.OrderID').alias('WWIOrderID'),
            col('so.BackorderOrderID').alias('WWIBackorderID'),
            col('sol.Description').alias('Description'),
            col('wpt.PackageTypeName').alias('Package'),
            col('sol.Quantity').alias('Quantity'),
            col('sol.UnitPrice').alias('UnitPrice'),
            col('sol.TaxRate').alias('TaxRate'),
            pysp_round(col('sol.Quantity') * col('sol.UnitPrice'), 2).alias("TotalExcludingTax"),
            pysp_round(col('sol.Quantity') * col('sol.UnitPrice') * col('sol.TaxRate') / 100, 2)
                .alias("TaxAmount"),
            (
             pysp_round(col('sol.Quantity') * col('sol.UnitPrice'), 2) +
             pysp_round(col('sol.Quantity') * col('sol.UnitPrice') * col('sol.TaxRate') / 100, 2)
            )
                .alias('TotalIncludingTax'),
            col('sc.DeliveryCityID').alias('WWICityID'),
            col('sc.CustomerID').alias('WWICustomerID'),
            col('sol.StockItemID').alias("WWIStockItemID"),
            col('so.SalespersonPersonID').alias('WWISalespersonID'),
            col('so.PickedByPersonID').alias('WWIPickerID'),
            when(
                col('sol.LastEditedWhen') > col('so.LastEditedWhen'),
                col('sol.LastEditedWhen')
            )
            .otherwise(col('so.LastEditedWhen')).alias('LastModifiedWhen'),
            col('LineageKey'),
            col('so.LoadDatetime')
        )
        .orderBy(col('so.OrderID'))
    )

    fact_order = (
        order_staging.alias('os')
        .join(
            dimension_city.alias('dci'),
            [
                col('os.WWICityID') == col('dci.WWICityID'),
                col('os.LastModifiedWhen') > col('dci.ValidFrom'),
                col('os.LastModifiedWhen') <= col('dci.ValidTo')
            ],
            'left'
        )
        .join(
            dimension_customer.alias('dcu'),
            [
                col('os.WWICustomerID') == col('dcu.WWICustomerID'),
                col('os.LastModifiedWhen') > col('dcu.ValidFrom'),
                col('os.LastModifiedWhen') <= col('dcu.ValidTo')
            ],
            'left'
        )
        .join(
            dimension_stock_item.alias('dsi'),
            [
                col('os.WWIStockItemID') == col('dsi.WWIStockItemID'),
                col('os.LastModifiedWhen') > col('dsi.ValidFrom'),
                col('os.LastModifiedWhen') <= col('dsi.ValidTo')
            ],
            'left'
        )
        .join(
            dimension_employee.alias('des'),
            [
                col('os.WWISalespersonID') == col('des.WWIEmployeeID'),
                col('os.LastModifiedWhen') > col('des.ValidFrom'),
                col('os.LastModifiedWhen') <= col('des.ValidTo')
            ],
            'left'
        )
        .join(
            dimension_employee.alias('dep'),
            [
                col('os.WWIPickerID') == col('dep.WWIEmployeeID'),
                col('os.LastModifiedWhen') > col('dep.ValidFrom'),
                col('os.LastModifiedWhen') <= col('dep.ValidTo')
            ],
            'left'
        )
        .drop_duplicates(['WWIOrderID', 'StockItemKey'])
        .selectExpr(
            "COALESCE(min(dci.CityKey) "
                "OVER(PARTITION BY dci.WWICityID ORDER BY dci.ValidFrom), 0) AS CityKey",
            "COALESCE(min(dcu.CustomerKey) "
                "OVER(PARTITION BY dcu.WWICustomerID ORDER BY dcu.ValidFrom), 0) AS CustomerKey",
            "COALESCE(min(dsi.StockItemKey) "
                "OVER(PARTITION BY dsi.WWIStockItemID ORDER BY dsi.ValidFrom), 0) AS StockItemKey",
            "os.OrderDateKey",
            "os.PickedDateKey",
            "COALESCE(min(des.EmployeeKey) "
                "OVER(PARTITION BY des.WWIEmployeeID ORDER BY des.ValidFrom), 0) AS SalespersonKey",
            "COALESCE(min(dep.EmployeeKey) "
                "OVER(PARTITION BY dep.WWIEmployeeID ORDER BY dep.ValidFrom), 0) AS PickerKey",
            "os.WWIOrderID",
            "os.WWIBackorderID",
            "os.Description",
            "os.Package",
            "os.Quantity",
            "os.UnitPrice",
            "os.TaxRate",
            "os.TotalExcludingTax",
            "os.TaxAmount",
            "os.TotalIncludingTax",
            "os.LineageKey",
            "os.LoadDatetime AS RawDataLoadDatetime"
        )
    )

    return zip_with_row_number_as_key(fact_order, key_name=primary_key_name, offset=offset, order_by='WWIOrderID')
    # NOTE for reliable id generation on really big datasets zip_with_key() from id_generator should be used
