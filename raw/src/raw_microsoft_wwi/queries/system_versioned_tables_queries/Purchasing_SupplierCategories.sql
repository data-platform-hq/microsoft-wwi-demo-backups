SELECT SupplierCategoryID,
        SupplierCategoryName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Purchasing.SupplierCategories
UNION
SELECT SupplierCategoryID,
        SupplierCategoryName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Purchasing.SupplierCategories_Archive

