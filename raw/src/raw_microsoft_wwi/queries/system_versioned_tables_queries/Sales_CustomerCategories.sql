SELECT CustomerCategoryID,
        CustomerCategoryName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Sales.CustomerCategories
UNION
SELECT CustomerCategoryID,
        CustomerCategoryName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Sales.CustomerCategories_Archive