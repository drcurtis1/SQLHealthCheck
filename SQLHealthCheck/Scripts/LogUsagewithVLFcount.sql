CREATE TABLE #VLFInfo(
	  [RecoveryUnitId] int NULL,
      [FileId] [tinyint] NULL,
      [FileSize] [bigint] NULL,
      [StartOffset] [bigint] NULL,
      [FSeqNo] [int] NULL,
      [Status] [tinyint] NULL,
      [Parity] [tinyint] NULL,
      [CreateLSN] [numeric](25, 0) NULL
) ON [PRIMARY]
 
CREATE TABLE #VLFCountResults(databasename sysname,fileid int, Free int, InUse int, VLFCount int)
 
EXEC sp_MSforeachdb N'Use [?];
INSERT INTO #VLFInfo
EXEC sp_executesql N''DBCC LOGINFO([?])''
;with vlfUse as
(
select max(db_name()) databasename,fileid,
sum(case when status = 0 then 1 else 0 end) as Free,
sum(case when status != 0 then 1 else 0 end) as InUse,
count(*) cnt
from #VLFInfo
group by fileid
)
INSERT INTO #VLFCountResults
select *  from vlfUse
TRUNCATE TABLE #VLFInfo
'
--select * from #VLFCountResults
 
;WITH DatbaseVLF AS(
SELECT 
DB_ID(dbs.[name]) AS DatabaseID,
dbs.[name] AS dbName, 
CONVERT(DECIMAL(18,2), p2.cntr_value/1024.0) AS [Log Size (MB)],
CONVERT(DECIMAL(18,2), p1.cntr_value/1024.0) AS [Log Size Used (MB)]
FROM sys.databases AS dbs WITH (NOLOCK)
INNER JOIN sys.dm_os_performance_counters AS p1  WITH (NOLOCK) ON dbs.name = p1.instance_name
INNER JOIN sys.dm_os_performance_counters AS p2 WITH (NOLOCK) ON dbs.name = p2.instance_name
WHERE p1.counter_name LIKE N'Log File(s) Used Size (KB)%' 
AND p2.counter_name LIKE N'Log File(s) Size (KB)%'
AND p2.cntr_value > 0 
)
SELECT	[dbName],
		[Log Size (MB)], 
		[Log Size Used (MB)], 
		[Log Size (MB)]-[Log Size Used (MB)] [Log Free (MB)], 
		cast([Log Size Used (MB)]/[Log Size (MB)]*100 as decimal(10,2)) [Log Space Used %],
		max(VLFCount) AS [Number of VLFs] ,
		max(Free) Free,
		Max(InUse) InUse
FROM DatbaseVLF AS vlf  
INNER JOIN #VLFCountResults b on vlf.dbName=b.databasename
GROUP BY dbName, [Log Size (MB)],[Log Size Used (MB)]
 
DROP TABLE #VLFInfo;
DROP TABLE #VLFCountResults