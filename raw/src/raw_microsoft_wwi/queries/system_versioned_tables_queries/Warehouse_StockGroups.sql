SELECT StockGroupID,
        StockGroupName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Warehouse.StockGroups
UNION
SELECT StockGroupID,
        StockGroupName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Warehouse.StockGroups_Archive