SELECT ColorID,
        ColorName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Warehouse.Colors
UNION
SELECT ColorID,
        ColorName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Warehouse.Colors_Archive