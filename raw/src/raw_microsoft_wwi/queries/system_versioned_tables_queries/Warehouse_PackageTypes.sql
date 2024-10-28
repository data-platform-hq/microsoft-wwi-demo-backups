SELECT PackageTypeID,
        PackageTypeName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Warehouse.PackageTypes
UNION
SELECT PackageTypeID,
        PackageTypeName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Warehouse.PackageTypes_Archive