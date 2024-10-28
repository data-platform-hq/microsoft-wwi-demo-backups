SELECT OrderLineID,
        OrderID,
        StockItemID,
        Description,
        PackageTypeID,
        Quantity,
        UnitPrice,
        TaxRate,
        PickedQuantity,
        PickingCompletedWhen,
        LastEditedBy,
        LastEditedWhen
    FROM Sales.OrderLines