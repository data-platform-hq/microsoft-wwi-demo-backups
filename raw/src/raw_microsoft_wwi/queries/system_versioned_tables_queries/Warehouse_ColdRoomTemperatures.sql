SELECT ColdRoomTemperatureID,
        ColdRoomSensorNumber,
        RecordedWhen,
        Temperature,
        ValidFrom,
        ValidTo
    FROM Warehouse.ColdRoomTemperatures
UNION
SELECT ColdRoomTemperatureID,
        ColdRoomSensorNumber,
        RecordedWhen,
        Temperature,
        ValidFrom,
        ValidTo
    FROM Warehouse.ColdRoomTemperatures_Archive