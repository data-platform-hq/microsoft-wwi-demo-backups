SELECT PaymentMethodID,
        PaymentMethodName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.PaymentMethods
UNION
SELECT PaymentMethodID,
        PaymentMethodName,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.PaymentMethods_Archive