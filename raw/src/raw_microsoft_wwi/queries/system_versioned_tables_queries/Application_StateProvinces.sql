SELECT StateProvinceID,
        StateProvinceCode,
        StateProvinceName,
        CountryID,
        SalesTerritory,
        CAST(Border AS varchar(MAX)) AS Border,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.StateProvinces
UNION
SELECT StateProvinceID,
        StateProvinceCode,
        StateProvinceName,
        CountryID,
        SalesTerritory,
        CAST(Border AS varchar(MAX)) AS Border,
        LatestRecordedPopulation,
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.StateProvinces_Archive