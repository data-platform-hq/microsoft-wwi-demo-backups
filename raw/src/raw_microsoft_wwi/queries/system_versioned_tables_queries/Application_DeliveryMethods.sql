SELECT DeliveryMethodID,
        DeliveryMethodName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.DeliveryMethods
UNION
SELECT DeliveryMethodID,
        DeliveryMethodName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.DeliveryMethods_Archive