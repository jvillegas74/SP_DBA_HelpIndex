use master
go

if exists (select 1 from sys.procedures where name = 'SP_DBA_HELPINDEX')
	DROP PROCEDURE [dbo].[SP_DBA_HELPINDEX] 
GO
CREATE  PROCEDURE [dbo].[SP_DBA_HELPINDEX]      
  @obj_name sysname ,    
 @dbname sysname = NULL    
AS      
-- V 5. - 2014-04-21    
-- V 5.1 - 2014-07-17 - Bug Fixed   
-- V 6. - 2019-01-28 - Add Compression and Partition Number  
 
SET NOCOUNT ON       
IF (object_id( 'tempdb..##T_INDEXES' ) IS NOT NULL) DROP TABLE ##T_INDEXES ;      
IF (object_id( 'tempdb..##T_INDEXES1' ) IS NOT NULL) DROP TABLE ##T_INDEXES1 ;      
IF (object_id( 'tempdb..##T_INDEXES_COL' ) IS NOT NULL) DROP TABLE ##T_INDEXES_COL ;      
      
CREATE TABLE ##T_INDEXES (      
  id int identity,      
  [schema_id] int,      
  table_obj_id int,      
  table_name sysname,      
  index_description varchar(200),      
  index_id int,      
  index_name varchar(200),      
  is_unique bit,      
  is_padded bit,      
  [allow_row_locks] bit,      
  [allow_page_locks] bit,      
  all_columns varchar(max),      
  all_included_columns varchar(max),    
  [schema_name] sysname,
  data_compression int,
  data_compression_desc varchar(50),
  partition_number int,
  rows int      
  )      
      
DECLARE @idx_id int,      
  @schema_id int,      
  @idx_name sysname,      
  @idx_desc varchar(200),      
  @tbl_obj_id int,      
  @tbl_name sysname,      
  @is_unique bit,      
  @is_padded bit,      
  @allow_row_locks bit,      
  @allow_page_locks bit,          
  @all_columns varchar(2000),      
  @all_included_columns varchar(2000),      
  @cmd varchar(max),    
  @schema_name sysname = NULL,
  @data_compression int,
  @data_compression_desc varchar(30),
  @Partition int ,
  @rows int   
    
IF @dbname IS NULL    
 SET @dbname = db_name()    
    
SET @obj_name=REPLACE(@obj_name,'[','')    
SET @obj_name=REPLACE(@obj_name,']','')    
    
SET @dbname=REPLACE(@dbname,'[','')    
SET @dbname=REPLACE(@dbname,']','')    
    
IF LEN(@obj_name) - LEN(REPLACE(@obj_name,'.',''))=1    
BEGIN    
 SET @schema_name = LEFT(@obj_name,CHARINDEX('.',@obj_name,0)-1)    
 SET @obj_name = SUBSTRING(@obj_name,CHARINDEX('.',@obj_name,0)+1,1000)    
END    
ELSE IF LEN(@obj_name) - LEN(REPLACE(@obj_name,'.',''))=2    
BEGIN    
 SET @dbname = LEFT(@obj_name,CHARINDEX('.',@obj_name,0)-1)    
 SET @schema_name = LEFT(@obj_name,CHARINDEX('.',@obj_name,0)-1)    
 SET @obj_name = SUBSTRING(@obj_name,CHARINDEX('.',@obj_name,0)+1,1000)    
    
 SET @schema_name = ltrim(rtrim(LEFT(@obj_name,CHARINDEX('.',@obj_name,0)-1)))    
 SET @obj_name = ltrim(rtrim(SUBSTRING(@obj_name,CHARINDEX('.',@obj_name,0)+1,1000)))    
END      
ELSE SET @schema_name = 'dbo'    

declare @fullobjname varchar(200)
set @fullobjname=@dbname+'.'+@schema_name+'.'+@obj_name

--select @fullobjname
--select object_id(@fullobjname)
IF  object_id(@fullobjname) IS NULL
BEGIN
SELECT 'TABLE '+@fullobjname+' DOES NOT EXIST'  as [Message]    
         RETURN -1 
END
        
select @cmd = 'SELECT o.object_id,o.schema_id,i.index_id,i.type_desc,i.name as i_name,o.name as o_name,i.is_unique,i.is_padded,i.allow_row_locks,i.allow_page_locks,s.name as schema_name,p.data_compression,p.data_compression_desc ,p.partition_number,p.rows      
   INTO ##T_INDEXES1      
   from '+@dbname+'.sys.indexes i       
   inner join '+@dbname+'.sys.objects o on i.object_id=o.object_id      
   inner join '+@dbname+'.sys.schemas s on o.schema_id=s.schema_id    
   inner join  '+@dbname+'.sys.partitions p on p.object_id=i.object_id and p.index_id=i.index_id
  where o.type = ''U''      
  and o.name = '''+@obj_name+''''+CASE WHEN @schema_name IS NOT NULL THEN ' and s.name = '''+@schema_name+'''' END    
       
BEGIN TRY      
     exec (@cmd)       
END TRY      
BEGIN CATCH      
          SELECT 'TABLE DOES NOT EXIST'  as [Message]    
         RETURN -1      
END CATCH      
    
DECLARE cursor_indexes_1 CURSOR FOR        
  SELECT object_id,schema_id,index_id,type_desc,i_name,o_name,is_unique,is_padded,allow_row_locks,allow_page_locks,[schema_name],data_compression,data_compression_desc,partition_number,rows
  from ##T_INDEXES1      
      
OPEN cursor_indexes_1      
      
FETCH NEXT FROM cursor_indexes_1      
INTO @tbl_obj_id,@schema_id,@idx_id,@idx_desc,@idx_name,@tbl_name,@is_unique,@is_padded,@allow_row_locks,@allow_page_locks  ,@schema_name ,@data_compression,@data_compression_desc,@partition,@rows
      
WHILE @@FETCH_STATUS = 0      
BEGIN      
  SET @all_columns = NULL        
  SET @all_included_columns = NULL      
          
  select @cmd = 'SELECT IDENTITY(int, 1,1) AS ID,      
  min(col.name) as col,c.is_included_column      
  INTO ##T_INDEXES_COL      
  from '+@dbname+'.sys.index_columns c      
  inner join '+@dbname+'.sys.objects o on c.object_id=o.object_id      
  inner join '+@dbname+'.sys.indexes i on c.object_id=i.object_id and c.index_id = i.index_id      
  inner join '+@dbname+'.sys.columns col on c.object_id=col.object_id and c.column_id=col.column_id      
  where o.type = ''U''       
  --and i.type =2      
  --and c.is_included_column =0      
  and i.index_id='+cast(@idx_id as varchar(50))+' --variable      
  and o.object_id='+cast(@tbl_obj_id as varchar(50))+' --variable      
  group by c.index_id,c.index_column_id,c.is_included_column      
  order by c.index_id,c.index_column_id,c.is_included_column'      
  exec (@cmd)        
        
  SELECT @all_columns = CASE WHEN @all_columns is null then ''        
    else @all_columns + ', '        
     end         
   +  col      
  from ##T_INDEXES_COL where is_included_column = 0      
  order by id      
        
  SELECT @all_included_columns = CASE WHEN @all_included_columns is null then ''        
    else @all_included_columns + ', '        
     end         
   +  col      
  from ##T_INDEXES_COL where is_included_column = 1      
  order by id       
        
  IF (object_id( 'tempdb..##T_INDEXES_COL' ) IS NOT NULL) DROP TABLE ##T_INDEXES_COL ;      
  IF (object_id( 'tempdb..##T_INDEXES_INCOL' ) IS NOT NULL) DROP TABLE ##T_INDEXES_INCOL ;      
        
  INSERT INTO ##T_INDEXES ([schema_id],table_obj_id,table_name,index_description,index_id,index_name,is_unique,is_padded,[allow_row_locks],[allow_page_locks],all_columns,all_included_columns,[schema_name],data_compression,data_compression_desc,partition_number,rows)
        
  SELECT @schema_id,@tbl_obj_id,@tbl_name,@idx_desc,@idx_id,@idx_name,@is_unique,@is_padded,@allow_row_locks,@allow_page_locks,@all_columns,@all_included_columns,@schema_name ,@data_compression,@data_compression_desc,@partition,@rows
          
  FETCH NEXT FROM cursor_indexes_1      
  INTO @tbl_obj_id,@schema_id,@idx_id,@idx_desc,@idx_name,@tbl_name,@is_unique,@is_padded,@allow_row_locks,@allow_page_locks ,@schema_name ,@data_compression  ,@data_compression_desc,@partition ,@rows   
END      
      
CLOSE cursor_indexes_1      
DEALLOCATE cursor_indexes_1       
        
SELECT [schema_id],table_obj_id,[schema_name],table_name,index_description,index_id,index_name,all_columns,all_included_columns,is_unique,is_padded,[allow_row_locks],[allow_page_locks],cast(data_compression as varchar(1))+'-'+data_compression_desc as compression,partition_number  FROM ##T_INDEXES  ORDER BY index_id    
 
 select @rows as [Row_Count]
        
      
IF (object_id( 'tempdb..##T_INDEXES' ) IS NOT NULL) DROP TABLE ##T_INDEXES ;      
IF (object_id( 'tempdb..##T_INDEXES1' ) IS NOT NULL) DROP TABLE ##T_INDEXES1 ;      
IF (object_id( 'tempdb..##T_INDEXES_COL' ) IS NOT NULL) DROP TABLE ##T_INDEXES_COL ;      
GO