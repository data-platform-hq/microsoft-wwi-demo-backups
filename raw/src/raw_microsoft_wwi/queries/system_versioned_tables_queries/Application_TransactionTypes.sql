SELECT TransactionTypeID,
        TransactionTypeName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.TransactionTypes
UNION
SELECT TransactionTypeID,
        TransactionTypeName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.TransactionTypes_Archive