SELECT InvoiceLineID,
        InvoiceID,
        StockItemID,
        Description,
        PackageTypeID,
        Quantity,
        UnitPrice,
        TaxRate,
        TaxAmount,
        LineProfit,
        ExtendedPrice,
        LastEditedBy,
        LastEditedWhen
    FROM Sales.InvoiceLines