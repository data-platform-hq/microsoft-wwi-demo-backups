SELECT StockItemID,
        QuantityOnHand,
        BinLocation,
        LastStocktakeQuantity,
        LastCostPrice,
        ReorderLevel,
        TargetStockLevel,
        LastEditedBy,
        LastEditedWhen
    FROM Warehouse.StockItemHoldings