SELECT CityID, 
        CityName, 
        StateProvinceID, 
        CAST(Location AS varchar(100)) AS Location, 
        LatestRecordedPopulation, 
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.Cities
UNION
SELECT CityID, 
        CityName, 
        StateProvinceID, 
        CAST(Location AS varchar(100)) AS Location, 
        LatestRecordedPopulation, 
        LastEditedBy,
        ValidFrom,
        ValidTo
    FROM Application.Cities_Archive