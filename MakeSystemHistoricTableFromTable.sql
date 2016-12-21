drop procedure if exists MakeSystemHistoricTableFromTable 
go

create procedure MakeSystemHistoricTableFromTable 
  @TableName nvarchar(128)
, @SchemaName nvarchar(128) = N'dbo'
, @HistoryTableName nvarchar(128) = null
, @HistorySchemaName nvarchar(128) = null
, @StartTimeFunction nvarchar(128) = N'SYSUTCDATETIME()'
, @PrintDDL bit = 'True'
, @debug bit = 'False'
as 
begin

/* 
Licensed under MIT License (MIT)
Copyright (c) 2015 Kennie Nybo Pontoppidan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.  
*/

  declare @ddl_string nvarchar(2000)
        , @error_message nvarchar(1000)
		, @TableFullPath nvarchar(500)
		, @HistoryTableFullPath nvarchar(500)
        , @sql_version numeric(4,2)
		, @temporal_type tinyint
		, @has_primary_key_p bit
  ;

  set @TableFullPath = '[' + @SchemaName + '].[' + @TableName + ']'

  if @debug = 'True' print 'TableFullPath is ' + @TableFullPath;

  -- check if table exists (it should)
  if object_id(@TableFullPath, N'U') is null
    begin
      set @error_message = N'The table ' + @TableFullPath + ' does not exist (or you do not have access to it)';
      THROW 51000, @error_message, 1;
	end

  if @HistoryTableName is not null 
    set @HistoryTableFullPath = '[' + 
								case
									when @HistorySchemaName is null then @SchemaName
									else @HistorySchemaName
								end + 
								'].[' + 
								@HistoryTableName + 
								']'
  else 
    set @HistoryTableFullPath = null

  print 'HistoryTableFullPath is ' + @HistoryTableFullPath

  -- check if history table exists (it should not)
  if object_id(@HistoryTableFullPath, N'U') is not null
    begin
      set @error_message = N'The history table ' + @HistoryTableFullPath + ' already exists. It cannot be used as the history table for ' + @TableName;
      THROW 51000, @error_message, 1;
	end

  -- check if temporal is supported
  select @sql_version = left(cast(serverproperty('productversion') as varchar), 4)
  if (@sql_version < 13.0) 
    begin
	  set @error_message = N'This version of SQL Server does not support temporal tables';
      THROW 51000, @error_message, 1;
	end

  -- check if table already has enabled system history
  /*
    0 = NON_TEMPORAL_TABLE
    1 = HISTORY_TABLE
    2 = SYSTEM_VERSIONED_TEMPORAL_TABLE
  */
  select @temporal_type = temporal_type
    from sys.tables 
   where schema_id = schema_id(@schemaName)
     and name = @TableName

  if (@temporal_type <> 0) 
    begin
	  set @error_message = N'This table is already configured as a temporal table';
      THROW 51000, @error_message, 1;
	end

  select @has_primary_key_p = count(*) 
    from sys.tables as t inner join sys.indexes as i on (t.object_id = i.object_id)
   where t.schema_id = schema_id(@schemaName)
     and t.name = @TableName
	 and i.is_primary_key = 'True'

  -- check for primary key
  if (@has_primary_key_p <> 'True') 
    begin
	  set @error_message = N'Table ' + @TableName + ' must have a primary key defined';
      THROW 51000, @error_message, 1;
	end

  -- create alter table ddl
  set @ddl_string = 
'
ALTER TABLE ' + @TableFullPath + ' 
  ADD SysStartTime datetime2(0) GENERATED ALWAYS AS ROW START HIDDEN  
        CONSTRAINT [DF_' + @TableName + '_SysStart] 
		DEFAULT ' + @StartTimeFunction + '
    , SysEndTime datetime2(0) GENERATED ALWAYS AS ROW END HIDDEN  
        CONSTRAINT [DF_' + @TableName + '_SysEnd] 
		DEFAULT CONVERT(datetime2 (0), ''9999-12-31 23:59:59'')
	, PERIOD FOR SYSTEM_TIME (SysStartTime, SysEndTime) 
;'

  set @ddl_string += 
'

ALTER TABLE ' + @TableFullPath + ' 
  SET ( 
    SYSTEM_VERSIONING = ON ' + 
case 
  when @HistoryTableFullPath is not null then '(HISTORY_TABLE = ' + @HistoryTableFullPath + ')'
  else ''
end + '
  );
'

  if (@debug = 1 or @PrintDDL = 1) print @ddl_string

  exec sp_executesql @ddl_string

end
