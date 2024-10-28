RESTORE DATABASE WideWorldImporters
FROM DISK = '/opt/mssql/data/WideWorldImporters-Full.bak'
WITH MOVE 'WWI_Primary' TO '/usr/mssql/data/backup/WWI_Primary.mdf',
    MOVE 'WWI_UserData' TO '/usr/mssql/data/backup/WWI_UserData.ndf',
	MOVE 'WWI_Log' TO '/usr/mssql/data/backup/WWI_Log.ldf',
	MOVE 'WWI_InMemory_Data_1' TO '/usr/mssql/data/backup/WWI_InMemory_Data_1'
GO	
	
RESTORE DATABASE WideWorldImportersDW
FROM DISK = '/opt/mssql/data/WideWorldImportersDW-Full.bak'
WITH MOVE 'WWI_Primary' TO '/usr/mssql/data/backupDW/WWI_Primary.mdf',
    MOVE 'WWI_UserData' TO '/usr/mssql/data/backupDW/WWI_UserData.ndf',
	MOVE 'WWI_Log' TO '/usr/mssql/data/backupDW/WWI_Log.ldf',
	MOVE 'WWIDW_InMemory_Data_1' TO '/usr/mssql/data/backupDW/WWIDW_InMemory_Data_1'
GO