SELECT SystemParameterID,
        DeliveryAddressLine1,
        DeliveryAddressLine2,
        DeliveryCityID,
        DeliveryPostalCode,
        CAST(DeliveryLocation AS varchar(100)) AS DeliveryLocation,
        PostalAddressLine1,
        PostalAddressLine2,
        PostalCityID,
        PostalPostalCode,
        ApplicationSettings,
        LastEditedBy,
        LastEditedWhen
    FROM Application.SystemParameters