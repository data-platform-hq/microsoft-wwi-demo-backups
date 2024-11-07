import datetime
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

from data_product_microsoft_wwi.transformers.id_generator import zip_with_row_number_as_key


def transform_fact_sale(invoices: DataFrame,
                        invoice_lines: DataFrame,
                        customers: DataFrame,
                        stock_items: DataFrame,
                        package_types: DataFrame,
                        dimension_employee: DataFrame,
                        dimension_city: DataFrame,
                        dimension_customer: DataFrame,
                        dimension_stock_item: DataFrame,
                        since: Optional[datetime.datetime] = None,
                        to: Optional[datetime.datetime] = None,
                        offset: int = 0,
                        primary_key_name: str = 'SaleKey') -> DataFrame:
    if since and to:
        invoices = invoices.where((col('LoadDatetime') > since) & (col('LoadDatetime') <= to))

    sale_staging = (
        invoices.alias('i')
        .join(
            invoice_lines.alias('il'),
            col('i.InvoiceId') == col('il.InvoiceId'),
            'inner')
        .join(
            stock_items.alias('si'),
            col('il.StockItemID') == col('si.StockItemID'),
            'inner')
        .join(
            package_types.alias('pt'),
            col('il.PackageTypeID') == col('pt.PackageTypeID'),
            'inner')
        .join(
            customers.alias('c'),
            col('i.CustomerID') == col('c.CustomerID'),
            'inner')
        .join(
            customers.alias('bt'),
            col('i.BillToCustomerID') == col('bt.CustomerID'),
            'inner')
        .withColumn('TotalDryItems',
                    when(col('si.IsChillerStock') == 0, col('il.Quantity')).otherwise(0))
        .withColumn('TotalChillerItems',
                    when(col('si.IsChillerStock') != 0, col('il.Quantity')).otherwise(0))
        .withColumn('LastModifiedWhen',
                    when(col('il.LastEditedWhen') > col('i.LastEditedWhen'), col('il.LastEditedWhen'))
                    .otherwise(col('i.LastEditedWhen')))
        .withColumn('LineageKey', lit(11))
        .select(
            col('i.InvoiceDate').cast('date').alias('InvoiceDateKey'),
            col('i.ConfirmedDeliveryTime').cast('date').alias('DeliveryDateKey'),
            col('i.InvoiceID').alias('WWIInvoiceID'),
            col('il.Description').alias('Description'),
            col('pt.PackageTypeName').alias('Package'),
            col('il.Quantity').alias('Quantity'),
            col('il.UnitPrice').alias('UnitPrice'),
            col('il.TaxRate').alias('TaxRate'),
            (col('il.ExtendedPrice') - col('il.TaxAmount')).alias('TotalExcludingTax'),
            col('il.TaxAmount').alias('TaxAmount'),
            col('il.LineProfit').alias('Profit'),
            col('il.ExtendedPrice').alias('TotalIncludingTax'),
            col('TotalDryItems'),
            col('TotalChillerItems'),
            col('c.DeliveryCityID').alias('WWICityID'),
            col('i.CustomerID').alias('WWICustomerID'),
            col('i.BillToCustomerID').alias('WWIBillToCustomerID'),
            col('il.StockItemID').alias('WWIStockItemID'),
            col('i.SalespersonPersonID').alias('WWISalespersonID'),
            col('LineageKey'),
            col('LastModifiedWhen'),
            col('i.LoadDatetime')
        ).orderBy(col('WWIInvoiceID'), col('InvoiceLineID'))
    )

    fact_sale = (
        sale_staging.alias('s')
        .join(
            dimension_city.alias('ci'),
            [
                col('ci.WWICityID') == col('s.WWICityID')
            ],
            'left')
        .join(
            dimension_customer.alias('cu'),
            [
                col('cu.WWICustomerID') == col('s.WWICustomerID'),
                col('s.LastModifiedWhen') > col('cu.ValidFrom'),
                col('s.LastModifiedWhen') <= col('cu.ValidTo')
            ],
            'left')
        .join(
            dimension_customer.alias('cus'),
            [
                col('cus.WWICustomerID') == col('s.WWIBillToCustomerID'),
                col('s.LastModifiedWhen') > col('cus.ValidFrom'),
                col('s.LastModifiedWhen') <= col('cus.ValidTo')
            ],
            'left')
        .join(
            dimension_stock_item.alias('si'),
            [
                col('si.WWIStockItemID') == col('s.WWIStockItemID'),
                col('s.LastModifiedWhen') > col('si.ValidFrom'),
                col('s.LastModifiedWhen') <= col('si.ValidTo')
            ],
            'left')
        .join(
            dimension_employee.alias('e'),
            [
                col('e.WWIEmployeeID') == col('s.WWISalespersonID'),
                col('s.LastModifiedWhen') > col('e.ValidFrom'),
                col('s.LastModifiedWhen') <= col('e.ValidTo')
            ],
            'left')
        .drop_duplicates(['WWIInvoiceID', 'StockItemKey'])
        .selectExpr(
            "COALESCE(min(ci.CityKey) OVER(PARTITION BY ci.WWICityID ORDER BY ci.ValidFrom), 0) AS CityKey",
            "COALESCE(min(cu.CustomerKey) OVER(PARTITION BY cu.WWICustomerID ORDER BY cu.ValidFrom), 0) AS CustomerKey",
            "COALESCE(min(cus.CustomerKey) OVER(PARTITION BY cus.WWICustomerID order by cus.ValidFrom), 0) AS BillToCustomerKey",
            "COALESCE(min(si.StockItemKey) OVER(PARTITION BY si.WWIStockItemID ORDER BY si.ValidFrom), 0) AS StockItemKey",
            "s.InvoiceDateKey",
            "s.DeliveryDateKey",
            "COALESCE(min(e.EmployeeKey) OVER(PARTITION BY e.WWIEmployeeID, e.ValidFrom), 0) AS SalespersonKey",
            "s.WWIInvoiceID",
            "s.Description",
            "s.Package",
            "s.Quantity",
            "s.UnitPrice",
            "s.TaxRate",
            "s.TotalExcludingTax",
            "s.TaxAmount",
            "s.Profit",
            "s.TotalIncludingTax",
            "s.TotalDryItems",
            "s.TotalChillerItems",
            "s.LineageKey",
            "s.LoadDatetime AS RawDataLoadDatetime"
        )
    )

    return zip_with_row_number_as_key(fact_sale, key_name=primary_key_name, offset=offset, order_by='WWIInvoiceID')
    # NOTE for reliable id generation on really big datasets zip_with_key() from id_generator should be used
