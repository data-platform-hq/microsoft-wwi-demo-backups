SELECT BuyingGroupID,
        BuyingGroupName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Sales.BuyingGroups
UNION
SELECT BuyingGroupID,
        BuyingGroupName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Sales.BuyingGroups_Archive