SELECT PurchaseOrderLineID,
        PurchaseOrderID,
        StockItemID,
        OrderedOuters,
        Description,
        ReceivedOuters,
        PackageTypeID,
        ExpectedUnitPricePerOuter,
        LastReceiptDate,
        IsOrderLineFinalized,
        LastEditedBy,
        LastEditedWhen
    FROM Purchasing.PurchaseOrderLines
