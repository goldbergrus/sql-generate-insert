USE [master]
GO
/****** Object:  StoredProcedure [dbo].[sp_generate_inserts]    Script Date: 2/15/2024 8:58:16 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER PROC [dbo].[sp_generate_inserts]
(
  @ObjectName nvarchar(261)
, @TargetObjectName nvarchar(261) = NULL
, @OmmitInsertColumnList bit = 0
, @UseSelectSyntax bit = 0
, @UseColumnAliasInSelect bit = 0
, @GenerateOneColumnPerLine bit = 0
, @TopExpression nvarchar(max) = NULL
, @SearchCondition nvarchar(max) = NULL
, @OrderByExpression nvarchar(max) = NULL
, @OmmitUnsupportedDataTypes bit = 1
, @PopulateTimestampColumn bit = 0
, @GenerateStatementTerminator bit = 1
, @ColumnsExclude nvarchar(max) = NULL
, @ColumnsInclude nvarchar(max) = NULL
)
AS
BEGIN


SET NOCOUNT ON;

DECLARE @CrLf char(2)
SET @CrLf = CHAR(13) + CHAR(10);
DECLARE @ColumnName sysname;
DECLARE @DataType sysname;
DECLARE @ColumnList nvarchar(max);
SET @ColumnList = N'';
DECLARE @SelectList nvarchar(max);
SET @SelectList = N'';
DECLARE @SelectStatement nvarchar(max);
SET @SelectStatement = N'';
DECLARE @OmittedColumnList nvarchar(max);
SET @OmittedColumnList = N'';
DECLARE @InsertSql nvarchar(max);
SET @InsertSql = N'INSERT INTO ' + COALESCE(@TargetObjectName,@ObjectName);
DECLARE @ValuesSql nvarchar(max);
SET @ValuesSql = N'VALUES (';
DECLARE @SelectSql nvarchar(max);
SET @SelectSql = N'SELECT ';
DECLARE @TableData table (TableRow nvarchar(max));
DECLARE @Results table (TableRow nvarchar(max));
DECLARE @TableRow nvarchar(max);
DECLARE @RowNo int;

IF PARSENAME(@ObjectName,3) IS NOT NULL
  OR PARSENAME(@ObjectName,4) IS NOT NULL
BEGIN
  RAISERROR(N'Server and database names are not allowed to specify in @ObjectName parameter. Required format is [schema_name.]object_name',16,1);
  RETURN -1;
END

IF OBJECT_ID(@ObjectName,N'U') IS NULL -- USER_TABLE
BEGIN
  RAISERROR(N'User table %s not found or insuficient permission to query the provided object.',16,1,@ObjectName);
  RETURN -1;
END

IF NOT EXISTS (
  SELECT 1
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE IN ('BASE TABLE','VIEW')
    AND TABLE_NAME = PARSENAME(@ObjectName,1)
    AND (TABLE_SCHEMA = PARSENAME(@ObjectName,2)
      OR PARSENAME(@ObjectName,2) IS NULL)
) AND NOT EXISTS (
  SELECT *
  FROM INFORMATION_SCHEMA.ROUTINES
  WHERE ROUTINE_TYPE IN ('FUNCTION')
    AND DATA_TYPE = 'TABLE'
    AND SPECIFIC_NAME = PARSENAME(@ObjectName,1)
    AND (SPECIFIC_SCHEMA = PARSENAME(@ObjectName,2)
      OR PARSENAME(@ObjectName,2) IS NULL)
)
BEGIN
  RAISERROR(N'User table %s not found or insuficient permission to query the provided object.',16,1,@ObjectName);
  RETURN -1;
END

DECLARE ColumnCursor CURSOR LOCAL FAST_FORWARD FOR
SELECT 
 c.name ColumnName
,COALESCE(TYPE_NAME(c.system_type_id),t.name) DataType
FROM sys.objects o
JOIN sys.columns c ON c.object_id = o.object_id
LEFT JOIN sys.types t ON t.system_type_id = c.system_type_id
					 AND t.user_type_id = c.user_type_id
OUTER APPLY (
	select i.is_primary_key, ic.key_ordinal
	from sys.indexes i 
	JOIN sys.index_columns ic ON ic.object_id = o.object_id
							 AND ic.index_id = i.index_id
							 AND ic.column_id = c.column_id
	WHERE i.object_id = o.object_id and i.is_primary_key = 1
) as pk

WHERE o.type = N'U' -- U = USER_TABLE
AND o.object_id = OBJECT_ID(@ObjectName) --OR o.name = @ObjectName) ???
AND COLUMNPROPERTY(c.object_id,c.name,'IsComputed') != 1
AND c.name not in (select value from STRING_SPLIT(@ColumnsExclude,','))
AND (c.name in (select value from STRING_SPLIT(@ColumnsInclude,',')) or @ColumnsInclude is null)

ORDER BY pk.is_primary_key desc,  COLUMNPROPERTY(c.object_id,c.name,'ordinal')
FOR READ ONLY;

OPEN ColumnCursor;
FETCH NEXT FROM ColumnCursor INTO @ColumnName,@DataType;

WHILE @@FETCH_STATUS = 0
BEGIN
  -- Handle different data types
  DECLARE @ColumnExpression nvarchar(max);
  SET @ColumnExpression = 
    CASE
    WHEN @DataType IN ('char','varchar','text','uniqueidentifier')
    THEN N'ISNULL(''''''''+REPLACE(CONVERT(varchar(max),'+  QUOTENAME(@ColumnName) + N'),'''''''','''''''''''')+'''''''',''NULL'') COLLATE database_default'
      
    WHEN @DataType IN ('nchar','nvarchar','sysname','ntext','sql_variant','xml')
    THEN N'ISNULL(''N''''''+REPLACE(CONVERT(nvarchar(max),'+  QUOTENAME(@ColumnName) + N'),'''''''','''''''''''')+'''''''',''NULL'') COLLATE database_default'
      
    WHEN @DataType IN ('int','bigint','smallint','tinyint','decimal','numeric','bit')
    THEN N'ISNULL(CONVERT(varchar(max),'+  QUOTENAME(@ColumnName) + N'),''NULL'') COLLATE database_default'
      
    WHEN @DataType IN ('float','real','money','smallmoney')
    THEN N'ISNULL(CONVERT(varchar(max),'+  QUOTENAME(@ColumnName) + N',2),''NULL'') COLLATE database_default'
      
    WHEN @DataType IN ('datetime','smalldatetime','date','time','datetime2','datetimeoffset')
    THEN N'''CONVERT('+@DataType+',''+ISNULL(''''''''+CONVERT(varchar(max),'+  QUOTENAME(@ColumnName) + N',121)+'''''''',''NULL'') COLLATE database_default' + '+'',121)'''

    WHEN @DataType IN ('rowversion','timestamp')
    THEN
      CASE WHEN @PopulateTimestampColumn = 1
      THEN N'''CONVERT(varbinary(max),''+ISNULL(''''''''+CONVERT(varchar(max),CONVERT(varbinary(max),'+  QUOTENAME(@ColumnName) + N'),1)+'''''''',''NULL'') COLLATE database_default' + '+'',1)'''
      ELSE N'''NULL''' END

    WHEN @DataType IN ('binary','varbinary','image')
    THEN N'''CONVERT(varbinary(max),''+ISNULL(''''''''+CONVERT(varchar(max),CONVERT(varbinary(max),'+  QUOTENAME(@ColumnName) + N'),1)+'''''''',''NULL'') COLLATE database_default' + '+'',1)'''

    WHEN @DataType IN ('geography')
    -- convert geography to text: ?? column.STAsText();
    -- convert text to geography: ?? geography::STGeomFromText('LINESTRING(-122.360 47.656, -122.343 47.656 )', 4326);
    THEN NULL

    ELSE NULL END;

  IF @ColumnExpression IS NULL
    AND @OmmitUnsupportedDataTypes != 1
  BEGIN
    RAISERROR(N'Datatype %s is not supported. Use @OmmitUnsupportedDataTypes to exclude unsupported columns.',16,1,@DataType);
    RETURN -1;
  END

  IF @ColumnExpression IS NULL
  BEGIN
    SET @OmittedColumnList = @OmittedColumnList
      + CASE WHEN @OmittedColumnList != N'' THEN N'; ' ELSE N'' END
      + N'column ' + QUOTENAME(@ColumnName)
      + N', datatype ' + @DataType;
  END

  IF @ColumnExpression IS NOT NULL
  BEGIN
    SET @ColumnList = @ColumnList
      + CASE WHEN @ColumnList != N'' THEN N',' ELSE N'' END
      + QUOTENAME(@ColumnName)
      + CASE WHEN @GenerateOneColumnPerLine = 1 THEN @CrLf ELSE N'' END;
  
    SET @SelectList = @SelectList
      + CASE WHEN @SelectList != N'' THEN N'+'',''+' + @CrLf ELSE N'' END
      + @ColumnExpression
      + CASE WHEN @UseColumnAliasInSelect = 1 AND @UseSelectSyntax = 1 THEN N'+'' ' + QUOTENAME(@ColumnName) + N'''' ELSE N'' END
      + CASE WHEN @GenerateOneColumnPerLine = 1 THEN N'+CHAR(13)+CHAR(10)' ELSE N'' END;
  END

  FETCH NEXT FROM ColumnCursor INTO @ColumnName,@DataType;
END

CLOSE ColumnCursor;
DEALLOCATE ColumnCursor;

IF NULLIF(@ColumnList,N'') IS NULL
BEGIN
  RAISERROR(N'No columns to select.',16,1);
  RETURN -1;
END



  SET @SelectList = 
    N'''' + @InsertSql + N'''+' + @CrLf
    + CASE WHEN @OmmitInsertColumnList = 1 THEN N'' ELSE N'''(' + @ColumnList + N')''+' + @CrLf END
    + N'''' + @ValuesSql + N'''+' + @CrLf
    + @SelectList
    + CASE WHEN @UseSelectSyntax = 1 THEN N'' ELSE N'+' + @CrLf + N''')''' END
    + CASE WHEN @GenerateStatementTerminator = 1 THEN N'+'';''' ELSE N'' END
;


SET @SelectStatement = N'SELECT'
  + CASE WHEN NULLIF(@TopExpression,N'') IS NOT NULL THEN N' TOP ' + @TopExpression ELSE N'' END
  + @CrLf + @SelectList + @CrLf
  + N'FROM ' + @ObjectName
  + CASE WHEN NULLIF(@SearchCondition,N'') IS NOT NULL THEN @CrLf + N'WHERE ' + @SearchCondition ELSE N'' END
  + CASE WHEN NULLIF(@OrderByExpression,N'') IS NOT NULL THEN @CrLf + N'ORDER BY ' + @OrderByExpression ELSE N'' END
  + @CrLf + N';'
;

 EXECUTE (@SelectStatement);
END

