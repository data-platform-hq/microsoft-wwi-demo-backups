USE WideWorldImporters;

EXEC sys.sp_cdc_enable_db;

DECLARE @Schema AS VARCHAR(300)
DECLARE @Table AS VARCHAR(300) 
DECLARE @cdc_Role AS VARCHAR(300) 

SET @cdc_Role = NULL;


DECLARE cdc_cursor CURSOR FOR  
SELECT TABLE_SCHEMA, TABLE_NAME  FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';

OPEN cdc_cursor   
FETCH NEXT FROM cdc_cursor INTO @Schema ,    @Table

WHILE @@FETCH_STATUS = 0   
BEGIN   
 

EXECUTE sys.sp_cdc_enable_table @source_schema = @Schema,
								@source_name = @Table,
								@role_name = @cdc_Role,
                                @supports_net_changes = 0;
								   
FETCH NEXT FROM cdc_cursor INTO @Schema, @Table
END   
CLOSE cdc_cursor   
DEALLOCATE cdc_cursor 