Create PROCEDURE dbo.sp_sql_server_health_check
    @CheckUserDatabaseObjects TINYINT = 1 ,
    @CheckProcedureCache TINYINT = 0 ,
    @OutputType VARCHAR(20) = 'TABLE' ,
    @OutputProcedureCache TINYINT = 0 ,
    @CheckProcedureCacheFilter VARCHAR(10) = NULL ,
    @CheckServerInfo TINYINT = 0 ,
    @SkipChecksServer NVARCHAR(256) = NULL ,
    @SkipChecksDatabase NVARCHAR(256) = NULL ,
    @SkipChecksSchema NVARCHAR(256) = NULL ,
    @SkipChecksTable NVARCHAR(256) = NULL ,
    @IgnorePrioritiesBelow INT = NULL ,
    @IgnorePrioritiesAbove INT = NULL ,
    @OutputServerName NVARCHAR(256) = NULL ,
    @OutputDatabaseName NVARCHAR(256) = NULL ,
    @OutputSchemaName NVARCHAR(256) = NULL ,
    @OutputTableName NVARCHAR(256) = NULL ,
    @OutputXMLasNVARCHAR TINYINT = 0 ,
    @EmailRecipients VARCHAR(MAX) = NULL ,
    @EmailProfile sysname = NULL ,
    @SummaryMode TINYINT = 0 ,
    @Debug TINYINT = 0 ,
    @VersionDate DATETIME = NULL OUTPUT
AS
    SET NOCOUNT ON;
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	DECLARE @Version VARCHAR(30);
	SET @OutputType = UPPER(@OutputType);
IF @OutputType = 'SCHEMA'
	BEGIN
		SELECT FieldList = '[Priority] TINYINT, 
		[FindingsGroup] VARCHAR(50), [Finding] VARCHAR(200), 
		[DatabaseName] NVARCHAR(128),  [Details] 
		NVARCHAR(4000), [QueryPlan] NVARCHAR(MAX), [QueryPlanFiltered]
		 NVARCHAR(MAX), [CheckID] INT';

	END;
	ELSE
	BEGIN

		DECLARE @StringToExecute NVARCHAR(4000)
			,@curr_tracefilename NVARCHAR(500)
			,@base_tracefilename NVARCHAR(500)
			,@indx int
			,@query_result_separator CHAR(1)
			,@EmailSubject NVARCHAR(255)
			,@EmailBody NVARCHAR(MAX)
			,@EmailAttachmentFilename NVARCHAR(255)
			,@ProductVersion NVARCHAR(128)
			,@ProductVersionMajor DECIMAL(10,2)
			,@ProductVersionMinor DECIMAL(10,2)
			,@CurrentName NVARCHAR(128)
			,@CurrentDefaultValue NVARCHAR(200)
			,@CurrentCheckID INT
			,@CurrentPriority INT
			,@CurrentFinding VARCHAR(200)
			,@CurrentDetails NVARCHAR(4000)
			,@MsSinceWaitsCleared DECIMAL(38,0)
			,@CpuMsSinceWaitsCleared DECIMAL(38,0)
			,@ResultText NVARCHAR(MAX)
			,@crlf NVARCHAR(2)
			,@Processors int
			,@NUMANodes int
			,@MinServerMemory bigint
			,@MaxServerMemory bigint
			,@ColumnStoreIndexesInUse bit
			,@TraceFileIssue bit
			,@IsWindowsOperatingSystem BIT
			,@DaysUptime NUMERIC(23,2);

		SET @crlf = NCHAR(13) + NCHAR(10);
		SET @ResultText = 'SQL_Server_HealthCheck_Results: ' + @crlf;

		
		SELECT @DaysUptime = CAST(DATEDIFF(HOUR, create_date, GETDATE()) / 24. AS NUMERIC(23, 2))
		FROM   sys.databases
		WHERE  database_id = 2;
		
		IF @DaysUptime = 0
		    SET @DaysUptime = .01;
		
		IF OBJECT_ID('tempdb..#SQLCheckResults') IS NOT NULL
			DROP TABLE #SQLCheckResults;
		CREATE TABLE #SQLCheckResults
			(
			  ID INT IDENTITY(1, 1) ,
			  CheckID INT ,
			  DatabaseName NVARCHAR(128) ,
			  Priority TINYINT ,
			  FindingsGroup VARCHAR(50) ,
			  Finding VARCHAR(200) ,
			  Details NVARCHAR(4000) ,
			  QueryPlan [XML] NULL ,
			  QueryPlanFiltered [NVARCHAR](MAX) NULL
			);

		IF OBJECT_ID('tempdb..#TemporaryDatabaseResults') IS NOT NULL
			DROP TABLE #TemporaryDatabaseResults;
		CREATE TABLE #TemporaryDatabaseResults
			(
			  DatabaseName NVARCHAR(128) ,
			  Finding NVARCHAR(128)
			);

		IF OBJECT_ID('tempdb..#SkipChecks') IS NOT NULL
			DROP TABLE #SkipChecks;
		CREATE TABLE #SkipChecks
			(
			  DatabaseName NVARCHAR(128) ,
			  CheckID INT ,
			  ServerName NVARCHAR(128)
			);
		CREATE CLUSTERED INDEX IX_CheckID_DatabaseName ON #SkipChecks(CheckID, DatabaseName);

		IF @SkipChecksTable IS NOT NULL
			AND @SkipChecksSchema IS NOT NULL
			AND @SkipChecksDatabase IS NOT NULL
			BEGIN
				
				IF @Debug IN (1, 2) RAISERROR('Inserting SkipChecks', 0, 1) WITH NOWAIT;
				
				SET @StringToExecute = 'INSERT INTO #SkipChecks(DatabaseName, CheckID, ServerName )
				SELECT DISTINCT DatabaseName, CheckID, ServerName
				FROM ' + QUOTENAME(@SkipChecksDatabase) + '.' + QUOTENAME(@SkipChecksSchema) + '.' +
				 QUOTENAME(@SkipChecksTable)
					+ ' WHERE ServerName IS NULL OR ServerName = SERVERPROPERTY(''ServerName'') 
					OPTION (RECOMPILE);';
				EXEC(@StringToExecute);
			END;

		IF NOT EXISTS ( SELECT  1
							FROM    #SkipChecks
							WHERE   DatabaseName IS NULL AND CheckID = 106 )
							AND (select convert(int,value_in_use) from sys.configurations where 
							name = 'default trace enabled' ) = 1
			BEGIN
				-- Flag for Windows OS to help with Linux support
				IF EXISTS ( SELECT  1
								FROM    sys.all_objects
								WHERE   name = 'dm_os_host_info' )
					BEGIN
						SELECT @IsWindowsOperatingSystem = CASE WHEN host_platform 
						= 'Windows' THEN 1 ELSE 0 END FROM sys.dm_os_host_info ;
					END;
					ELSE
					BEGIN
						SELECT @IsWindowsOperatingSystem = 1 ;
					END;

					select @curr_tracefilename = [path] from sys.traces where is_default = 1 ;
					set @curr_tracefilename = reverse(@curr_tracefilename);

					-- Set the trace file path separator based on underlying OS
					IF (@IsWindowsOperatingSystem = 1)
					BEGIN
						select @indx = patindex('%\%', @curr_tracefilename) ;
						set @curr_tracefilename = reverse(@curr_tracefilename) ;
						set @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '\log.trc' ;
					END;
					ELSE
					BEGIN
						select @indx = patindex('%/%', @curr_tracefilename) ;
						set @curr_tracefilename = reverse(@curr_tracefilename) ;
						set @base_tracefilename = left( @curr_tracefilename,len(@curr_tracefilename) - @indx) + '/log.trc' ;
					END;

			END;

		
		IF @CheckUserDatabaseObjects = 1 AND EXISTS(SELECT * FROM sys.databases WHERE compatibility_level < 90)
		BEGIN
			SET @CheckUserDatabaseObjects = 0;
			PRINT 'Databases with compatibility level < 90 found, so setting @CheckUserDatabaseObjects = 0.';
			PRINT 'The database-level checks rely on CTEs, which are not supported in SQL 2000 compat level databases.';
			PRINT 'SELECT * FROM sys.databases WHERE compatibility_level < 90;';
			INSERT  INTO #SQLCheckResults
			( CheckID ,
				Priority ,
				FindingsGroup ,
				Finding ,
				Details
			)
			SELECT 204 AS CheckID ,
					0 AS Priority ,
					'Informational' AS FindingsGroup ,
					'@CheckUserDatabaseObjects Disabled' AS Finding ,
					'Since you have databases with compatibility_level < 90, we can''t run @CheckUserDatabaseObjects = 1. To find them: SELECT * FROM sys.databases WHERE compatibility_level < 90' AS Details;
		END;

		
		IF CAST(SERVERPROPERTY('Edition') AS NVARCHAR(1000)) LIKE N'%Express%'
			BEGIN
						INSERT INTO #SkipChecks (CheckID) VALUES (30); 
						INSERT INTO #SkipChecks (CheckID) VALUES (31); 
						INSERT INTO #SkipChecks (CheckID) VALUES (61); 
						INSERT INTO #SkipChecks (CheckID) VALUES (73); 
						INSERT INTO #SkipChecks (CheckID) VALUES (96); 
			END; 

		
		IF OBJECT_ID('tempdb..#ConfigurationDefaults') IS NOT NULL
			DROP TABLE #ConfigurationDefaults;
		CREATE TABLE #ConfigurationDefaults
			(
			  name NVARCHAR(128) ,
			  DefaultValue BIGINT,
			  CheckID INT
			);

        IF OBJECT_ID ('tempdb..#Recompile') IS NOT NULL
            DROP TABLE #Recompile;
        CREATE TABLE #Recompile(
            DBName varchar(200),
            ProcName varchar(300),
            RecompileFlag varchar(1),
            SPSchema varchar(50)
        );

		IF OBJECT_ID('tempdb..#DatabaseDefaults') IS NOT NULL
			DROP TABLE #DatabaseDefaults;
		CREATE TABLE #DatabaseDefaults
			(
				name NVARCHAR(128) ,
				DefaultValue NVARCHAR(200),
				CheckID INT,
		        Priority INT,
		        Finding VARCHAR(200),
		        Details NVARCHAR(4000)
			);

		IF OBJECT_ID('tempdb..#DatabaseScopedConfigurationDefaults') IS NOT NULL
			DROP TABLE #DatabaseScopedConfigurationDefaults;
		CREATE TABLE #DatabaseScopedConfigurationDefaults
			(ID INT IDENTITY(1,1), configuration_id INT, 
			[name] NVARCHAR(60), default_value sql_variant, 
			default_value_for_secondary sql_variant, CheckID INT, );

		IF OBJECT_ID('tempdb..#DBCCs') IS NOT NULL
			DROP TABLE #DBCCs;
		CREATE TABLE #DBCCs
			(
			  ID INT IDENTITY(1, 1)
					 PRIMARY KEY ,
			  ParentObject VARCHAR(255) ,
			  Object VARCHAR(255) ,
			  Field VARCHAR(255) ,
			  Value VARCHAR(255) ,
			  DbName NVARCHAR(128) NULL
			);

		IF OBJECT_ID('tempdb..#LogInfo2012') IS NOT NULL
			DROP TABLE #LogInfo2012;
		CREATE TABLE #LogInfo2012
			(
			  recoveryunitid INT ,
			  FileID SMALLINT ,
			  FileSize BIGINT ,
			  StartOffset BIGINT ,
			  FSeqNo BIGINT ,
			  [Status] TINYINT ,
			  Parity TINYINT ,
			  CreateLSN NUMERIC(38)
			);

		IF OBJECT_ID('tempdb..#LogInfo') IS NOT NULL
			DROP TABLE #LogInfo;
		CREATE TABLE #LogInfo
			(
			  FileID SMALLINT ,
			  FileSize BIGINT ,
			  StartOffset BIGINT ,
			  FSeqNo BIGINT ,
			  [Status] TINYINT ,
			  Parity TINYINT ,
			  CreateLSN NUMERIC(38)
			);

		IF OBJECT_ID('tempdb..#partdb') IS NOT NULL
			DROP TABLE #partdb;
		CREATE TABLE #partdb
			(
			  dbname NVARCHAR(128) ,
			  objectname NVARCHAR(200) ,
			  type_desc NVARCHAR(128)
			);

		IF OBJECT_ID('tempdb..#TraceStatus') IS NOT NULL
			DROP TABLE #TraceStatus;
		CREATE TABLE #TraceStatus
			(
			  TraceFlag VARCHAR(10) ,
			  status BIT ,
			  Global BIT ,
			  Session BIT
			);

		IF OBJECT_ID('tempdb..#driveInfo') IS NOT NULL
			DROP TABLE #driveInfo;
		CREATE TABLE #driveInfo
			(
			  drive NVARCHAR ,
			  SIZE DECIMAL(18, 2)
			);

		IF OBJECT_ID('tempdb..#dm_exec_query_stats') IS NOT NULL
			DROP TABLE #dm_exec_query_stats;
		CREATE TABLE #dm_exec_query_stats
			(
			  [id] [int] NOT NULL
						 IDENTITY(1, 1) ,
			  [sql_handle] [varbinary](64) NOT NULL ,
			  [statement_start_offset] [int] NOT NULL ,
			  [statement_end_offset] [int] NOT NULL ,
			  [plan_generation_num] [bigint] NOT NULL ,
			  [plan_handle] [varbinary](64) NOT NULL ,
			  [creation_time] [datetime] NOT NULL ,
			  [last_execution_time] [datetime] NOT NULL ,
			  [execution_count] [bigint] NOT NULL ,
			  [total_worker_time] [bigint] NOT NULL ,
			  [last_worker_time] [bigint] NOT NULL ,
			  [min_worker_time] [bigint] NOT NULL ,
			  [max_worker_time] [bigint] NOT NULL ,
			  [total_physical_reads] [bigint] NOT NULL ,
			  [last_physical_reads] [bigint] NOT NULL ,
			  [min_physical_reads] [bigint] NOT NULL ,
			  [max_physical_reads] [bigint] NOT NULL ,
			  [total_logical_writes] [bigint] NOT NULL ,
			  [last_logical_writes] [bigint] NOT NULL ,
			  [min_logical_writes] [bigint] NOT NULL ,
			  [max_logical_writes] [bigint] NOT NULL ,
			  [total_logical_reads] [bigint] NOT NULL ,
			  [last_logical_reads] [bigint] NOT NULL ,
			  [min_logical_reads] [bigint] NOT NULL ,
			  [max_logical_reads] [bigint] NOT NULL ,
			  [total_clr_time] [bigint] NOT NULL ,
			  [last_clr_time] [bigint] NOT NULL ,
			  [min_clr_time] [bigint] NOT NULL ,
			  [max_clr_time] [bigint] NOT NULL ,
			  [total_elapsed_time] [bigint] NOT NULL ,
			  [last_elapsed_time] [bigint] NOT NULL ,
			  [min_elapsed_time] [bigint] NOT NULL ,
			  [max_elapsed_time] [bigint] NOT NULL ,
			  [query_hash] [binary](8) NULL ,
			  [query_plan_hash] [binary](8) NULL ,
			  [query_plan] [xml] NULL ,
			  [query_plan_filtered] [nvarchar](MAX) NULL ,
			  [text] [nvarchar](MAX) COLLATE SQL_Latin1_General_CP1_CI_AS
									 NULL ,
			  [text_filtered] [nvarchar](MAX) COLLATE SQL_Latin1_General_CP1_CI_AS
											  NULL
			);

		IF OBJECT_ID('tempdb..#ErrorLog') IS NOT NULL
			DROP TABLE #ErrorLog;
		CREATE TABLE #ErrorLog
			(
			  LogDate DATETIME ,
			  ProcessInfo NVARCHAR(20) ,
			  [Text] NVARCHAR(1000)
			);

		IF OBJECT_ID('tempdb..#fnTraceGettable') IS NOT NULL
			DROP TABLE #fnTraceGettable;
		CREATE TABLE #fnTraceGettable
			(
			  TextData NVARCHAR(4000) ,
			  DatabaseName NVARCHAR(256) ,
			  EventClass INT ,
			  Severity INT ,
			  StartTime DATETIME ,
			  EndTime DATETIME ,
			  Duration BIGINT ,
			  NTUserName NVARCHAR(256) ,
			  NTDomainName NVARCHAR(256) ,
			  HostName NVARCHAR(256) ,
			  ApplicationName NVARCHAR(256) ,
			  LoginName NVARCHAR(256) ,
			  DBUserName NVARCHAR(256)
			 );

		IF OBJECT_ID('tempdb..#Instances') IS NOT NULL
			DROP TABLE #Instances;
		CREATE TABLE #Instances
            (
              Instance_Number NVARCHAR(MAX) ,
              Instance_Name NVARCHAR(MAX) ,
              Data_Field NVARCHAR(MAX)
            );

		IF OBJECT_ID('tempdb..#IgnorableWaits') IS NOT NULL
			DROP TABLE #IgnorableWaits;
		CREATE TABLE #IgnorableWaits (wait_type NVARCHAR(60));
		INSERT INTO #IgnorableWaits VALUES ('BROKER_EVENTHANDLER');
		INSERT INTO #IgnorableWaits VALUES ('BROKER_RECEIVE_WAITFOR');
		INSERT INTO #IgnorableWaits VALUES ('BROKER_TASK_STOP');
		INSERT INTO #IgnorableWaits VALUES ('BROKER_TO_FLUSH');
		INSERT INTO #IgnorableWaits VALUES ('BROKER_TRANSMITTER');
		INSERT INTO #IgnorableWaits VALUES ('CHECKPOINT_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('CLR_AUTO_EVENT');
		INSERT INTO #IgnorableWaits VALUES ('CLR_MANUAL_EVENT');
		INSERT INTO #IgnorableWaits VALUES ('CLR_SEMAPHORE');
		INSERT INTO #IgnorableWaits VALUES ('DBMIRROR_DBM_EVENT');
		INSERT INTO #IgnorableWaits VALUES ('DBMIRROR_DBM_MUTEX');
		INSERT INTO #IgnorableWaits VALUES ('DBMIRROR_EVENTS_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('DBMIRROR_WORKER_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('DBMIRRORING_CMD');
		INSERT INTO #IgnorableWaits VALUES ('DIRTY_PAGE_POLL');
		INSERT INTO #IgnorableWaits VALUES ('DISPATCHER_QUEUE_SEMAPHORE');
		INSERT INTO #IgnorableWaits VALUES ('FT_IFTS_SCHEDULER_IDLE_WAIT');
		INSERT INTO #IgnorableWaits VALUES ('FT_IFTSHC_MUTEX');
		INSERT INTO #IgnorableWaits VALUES ('HADR_CLUSAPI_CALL');
		INSERT INTO #IgnorableWaits VALUES ('HADR_FILESTREAM_IOMGR_IOCOMPLETION');
		INSERT INTO #IgnorableWaits VALUES ('HADR_LOGCAPTURE_WAIT');
		INSERT INTO #IgnorableWaits VALUES ('HADR_NOTIFICATION_DEQUEUE');
		INSERT INTO #IgnorableWaits VALUES ('HADR_TIMER_TASK');
		INSERT INTO #IgnorableWaits VALUES ('HADR_WORK_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('LAZYWRITER_SLEEP');
		INSERT INTO #IgnorableWaits VALUES ('LOGMGR_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('ONDEMAND_TASK_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('PARALLEL_REDO_DRAIN_WORKER');
		INSERT INTO #IgnorableWaits VALUES ('PARALLEL_REDO_LOG_CACHE');
		INSERT INTO #IgnorableWaits VALUES ('PARALLEL_REDO_TRAN_LIST');
		INSERT INTO #IgnorableWaits VALUES ('PARALLEL_REDO_WORKER_SYNC');
		INSERT INTO #IgnorableWaits VALUES ('PARALLEL_REDO_WORKER_WAIT_WORK');
		INSERT INTO #IgnorableWaits VALUES ('PREEMPTIVE_HADR_LEASE_MECHANISM');
		INSERT INTO #IgnorableWaits VALUES ('PREEMPTIVE_SP_SERVER_DIAGNOSTICS');
		INSERT INTO #IgnorableWaits VALUES ('QDS_ASYNC_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP');
		INSERT INTO #IgnorableWaits VALUES ('QDS_PERSIST_TASK_MAIN_LOOP_SLEEP');
		INSERT INTO #IgnorableWaits VALUES ('QDS_SHUTDOWN_QUEUE');
		INSERT INTO #IgnorableWaits VALUES ('REDO_THREAD_PENDING_WORK');
		INSERT INTO #IgnorableWaits VALUES ('REQUEST_FOR_DEADLOCK_SEARCH');
		INSERT INTO #IgnorableWaits VALUES ('SLEEP_SYSTEMTASK');
		INSERT INTO #IgnorableWaits VALUES ('SLEEP_TASK');
		INSERT INTO #IgnorableWaits VALUES ('SP_SERVER_DIAGNOSTICS_SLEEP');
		INSERT INTO #IgnorableWaits VALUES ('SQLTRACE_BUFFER_FLUSH');
		INSERT INTO #IgnorableWaits VALUES ('SQLTRACE_INCREMENTAL_FLUSH_SLEEP');
		INSERT INTO #IgnorableWaits VALUES ('UCS_SESSION_REGISTRATION');
		INSERT INTO #IgnorableWaits VALUES ('WAIT_XTP_OFFLINE_CKPT_NEW_LOG');
		INSERT INTO #IgnorableWaits VALUES ('WAITFOR');
		INSERT INTO #IgnorableWaits VALUES ('XE_DISPATCHER_WAIT');
		INSERT INTO #IgnorableWaits VALUES ('XE_LIVE_TARGET_TVF');
		INSERT INTO #IgnorableWaits VALUES ('XE_TIMER_EVENT');

		IF @Debug IN (1, 2) RAISERROR('Setting @MsSinceWaitsCleared', 0, 1) WITH NOWAIT;

        SELECT @MsSinceWaitsCleared = DATEDIFF(MINUTE, create_date, CURRENT_TIMESTAMP) * 60000.0
            FROM    sys.databases
            WHERE   name = 'tempdb';

		IF @MsSinceWaitsCleared * .9 > (SELECT MAX(wait_time_ms) FROM sys.dm_os_wait_stats 
		WHERE wait_type IN ('SP_SERVER_DIAGNOSTICS_SLEEP', 'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 
		'REQUEST_FOR_DEADLOCK_SEARCH', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'LAZYWRITER_SLEEP',
		 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'DIRTY_PAGE_POLL', 'LOGMGR_QUEUE'))
			BEGIN
				
				IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 185) WITH NOWAIT;

				SET @MsSinceWaitsCleared = (SELECT MAX(wait_time_ms) 
				FROM sys.dm_os_wait_stats WHERE wait_type IN ('SP_SERVER_DIAGNOSTICS_SLEEP', 
				'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'REQUEST_FOR_DEADLOCK_SEARCH',
				 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'LAZYWRITER_SLEEP',
				  'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'DIRTY_PAGE_POLL', 'LOGMGR_QUEUE'));
				IF @MsSinceWaitsCleared = 0 SET @MsSinceWaitsCleared = 1;
				INSERT  INTO #SQLCheckResults
						(   CheckID ,
							Priority ,
							FindingsGroup ,
							Finding ,
							Details
						)
					VALUES(		185,
								240,
								'Wait Stats',
								'Wait Stats Have Been Cleared',
								'Someone ran DBCC SQLPERF to clear sys.dm_os_wait_stats at approximately: ' 
									+ CONVERT(NVARCHAR(100), 
										DATEADD(MINUTE, (-1. * (@MsSinceWaitsCleared) / 1000. / 60.), GETDATE()), 120));
			END;

		IF @Debug IN (1, 2) RAISERROR('Setting @CpuMsSinceWaitsCleared', 0, 1) WITH NOWAIT;
		
		SELECT @CpuMsSinceWaitsCleared = @MsSinceWaitsCleared * scheduler_count
			FROM sys.dm_os_sys_info;

		IF @OutputType = 'CSV' OR @OutputType = 'MARKDOWN'
			SET @CheckProcedureCache = 0;

			IF @OutputType = 'MARKDOWN'
			SET @CheckServerInfo = 1;

				SELECT
			@OutputServerName = QUOTENAME(@OutputServerName),
			@OutputDatabaseName = QUOTENAME(@OutputDatabaseName),
			@OutputSchemaName = QUOTENAME(@OutputSchemaName),
			@OutputTableName = QUOTENAME(@OutputTableName);

		
		IF @Debug IN (1, 2) RAISERROR('Getting version information.', 0, 1) WITH NOWAIT;
		
		SET @ProductVersion = CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128));
		SELECT @ProductVersionMajor = SUBSTRING(@ProductVersion, 1,CHARINDEX('.', @ProductVersion) + 1 ),
			@ProductVersionMinor = PARSENAME(CONVERT(varchar(32), @ProductVersion), 2);
		
				IF ( ( SERVERPROPERTY('ServerName') NOT IN ( SELECT ServerName
													 FROM   #SkipChecks
													 WHERE  DatabaseName IS NULL
															AND CheckID IS NULL ) )
			 OR ( @SkipChecksTable IS NULL )
		   )
			BEGIN

								IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 1 )
					BEGIN

						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 1) WITH NOWAIT;

                        IF SERVERPROPERTY('EngineName') <> 8 
                            BEGIN
						    INSERT  INTO #SQLCheckResults
								    ( CheckID ,
								      DatabaseName ,
								      Priority ,
								      FindingsGroup ,
								      Finding ,
								      Details
								    )
								    SELECT  1 AS CheckID ,
										    d.[name] AS DatabaseName ,
										    1 AS Priority ,
										    'Backup' AS FindingsGroup ,
										    'Backups Not Performed Recently' AS Finding ,
										    'Last backed up: '
										    + COALESCE(CAST(MAX(b.backup_finish_date) 
										    AS VARCHAR(25)),'never') AS Details
								    FROM    master.sys.databases d
										    LEFT OUTER JOIN msdb.dbo.backupset b ON d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
																      AND b.type = 'D'
																      AND b.server_name = SERVERPROPERTY('ServerName') 
								    WHERE   d.database_id <> 2 
										    AND d.state NOT IN(1, 6, 10) 
										    AND d.is_in_standby = 0 
										    AND d.source_database_id IS NULL
										    AND d.name NOT IN ( SELECT DISTINCT
																      DatabaseName
															    FROM  #SkipChecks
															    WHERE CheckID IS NULL OR CheckID = 1)
										    
								    GROUP BY d.name
								    HAVING  MAX(b.backup_finish_date) <= DATEADD(dd,
																      -7, GETDATE())
                                            OR MAX(b.backup_finish_date) IS NULL;
                                END;

                        ELSE
                            BEGIN
						    INSERT  INTO #SQLCheckResults
								    ( CheckID ,
								      DatabaseName ,
								      Priority ,
								      FindingsGroup ,
								      Finding ,
								      Details
								    )
								    SELECT  1 AS CheckID ,
										    d.[name] AS DatabaseName ,
										    1 AS Priority ,
										    'Backup' AS FindingsGroup ,
										    'Backups Not Performed Recently' AS Finding ,
										    'Last backed up: '
										    + COALESCE(CAST(MAX(b.backup_finish_date) AS VARCHAR(25)),'never') AS Details
								    FROM    master.sys.databases d
										    LEFT OUTER JOIN msdb.dbo.backupset b ON d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
																      AND b.type = 'D'
								    WHERE   d.database_id <> 2  
										    AND d.state NOT IN(1, 6, 10) 
										    AND d.is_in_standby = 0 
										    AND d.source_database_id IS NULL 
										    AND d.name NOT IN ( SELECT DISTINCT
																      DatabaseName
															    FROM  #SkipChecks
															    WHERE CheckID IS NULL OR CheckID = 1)
										    
								    GROUP BY d.name
								    HAVING  MAX(b.backup_finish_date) <= DATEADD(dd,
																      -7, GETDATE())
                                            OR MAX(b.backup_finish_date) IS NULL;
                                END;



						

					END;

				
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 2 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 2) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  
								  Details
								)
										SELECT DISTINCT
										2 AS CheckID ,
										d.name AS DatabaseName ,
										1 AS Priority ,
										'Backup' AS FindingsGroup ,
										'Full Recovery Model w/o Log Backups' AS Finding ,
										( 'The ' + CAST(CAST((SELECT ((SUM([mf].[size]) * 8.) / 1024.) FROM sys.[master_files] AS [mf] WHERE [mf].[database_id] = d.[database_id] AND [mf].[type_desc] = 'LOG') AS DECIMAL(18,2)) AS VARCHAR(30)) + 'MB log file has not been backed up in the last week.' ) AS Details
								FROM    master.sys.databases d
								WHERE   d.recovery_model IN ( 1, 2 )
										AND d.database_id NOT IN ( 2, 3 )
										AND d.source_database_id IS NULL
										AND d.state NOT IN(1, 6, 10)
										AND d.is_in_standby = 0 
										AND d.source_database_id IS NULL 
										AND d.name NOT IN ( SELECT DISTINCT
																  DatabaseName
															FROM  #SkipChecks
															WHERE CheckID IS NULL OR CheckID = 2)
										AND NOT EXISTS ( SELECT *
														 FROM   msdb.dbo.backupset b
														 WHERE  d.name COLLATE SQL_Latin1_General_CP1_CI_AS = b.database_name COLLATE SQL_Latin1_General_CP1_CI_AS
																AND b.type = 'L'
																AND b.backup_finish_date >= DATEADD(dd,
																  -7, GETDATE()) );
					END;

								IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 8 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 8) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
							(CheckID, Priority,
							FindingsGroup,
							Finding, 
							Details)
					  SELECT 8 AS CheckID,
					  230 AS Priority,
					  ''Security'' AS FindingsGroup,
					  ''Server Audits Running'' AS Finding,
					  
					  (''SQL Server built-in audit functionality is being used by server audit: '' + [name]) AS Details FROM sys.dm_server_audit_status  OPTION (RECOMPILE);';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;
					END;

				
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 93 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 93) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  								  Details
								)
								SELECT
										93 AS CheckID ,
										1 AS Priority ,
										'Backup' AS FindingsGroup ,
										'Backing Up to Same Drive Where Databases Reside' AS Finding ,
										
										CAST(COUNT(1) AS VARCHAR(50)) + ' backups done on drive '
										+ UPPER(LEFT(bmf.physical_device_name, 3))
										+ ' in the last two weeks, where database files also live. This represents a serious risk if that array fails.' Details
								FROM    msdb.dbo.backupmediafamily AS bmf
										INNER JOIN msdb.dbo.backupset AS bs ON bmf.media_set_id = bs.media_set_id
																  AND bs.backup_start_date >= ( DATEADD(dd,
																  -14, GETDATE()) )
										
										LEFT OUTER JOIN msdb.dbo.restorehistory rh ON bs.database_name = rh.destination_database_name AND rh.restore_date > DATEADD(dd, -14, GETDATE())
								WHERE   UPPER(LEFT(bmf.physical_device_name, 3)) <> 'HTT' AND
                                        UPPER(LEFT(bmf.physical_device_name COLLATE SQL_Latin1_General_CP1_CI_AS, 3)) IN (
										SELECT DISTINCT
												UPPER(LEFT(mf.physical_name COLLATE SQL_Latin1_General_CP1_CI_AS, 3))
										FROM    sys.master_files AS mf )
										AND rh.destination_database_name IS NULL
								GROUP BY UPPER(LEFT(bmf.physical_device_name, 3));
					END;

					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 119 )
						AND EXISTS ( SELECT *
									 FROM   sys.all_objects o
									 WHERE  o.name = 'dm_database_encryption_keys' )
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 119) WITH NOWAIT;

							SET @StringToExecute = 'INSERT INTO #SQLCheckResults 
							(CheckID, Priority, FindingsGroup, Finding, DatabaseName, Details)
								SELECT 119 AS CheckID,
								1 AS Priority,
								''Backup'' AS FindingsGroup,
								''TDE Certificate Not Backed Up Recently'' AS Finding,
								db_name(dek.database_id) AS DatabaseName,
								''The certificate '' + c.name + '' is used to encrypt database '' + db_name(dek.database_id) + ''. Last backup date: '' + COALESCE(CAST(c.pvt_key_last_backup_date AS VARCHAR(100)), ''Never'') AS Details
								FROM sys.certificates c INNER JOIN sys.dm_database_encryption_keys dek ON c.thumbprint = dek.encryptor_thumbprint
								WHERE pvt_key_last_backup_date IS NULL OR pvt_key_last_backup_date <= DATEADD(dd, -30, GETDATE())  OPTION (RECOMPILE);';
							
							IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
							IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
							
							EXECUTE(@StringToExecute);
						END;

                     IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 202 )
						AND EXISTS ( SELECT *
									 FROM   sys.all_columns c
									 WHERE  c.name = 'pvt_key_last_backup_date' )
						AND EXISTS ( SELECT *
									 FROM   msdb.INFORMATION_SCHEMA.COLUMNS c
									 WHERE  c.TABLE_NAME = 'backupset' AND c.COLUMN_NAME = 'encryptor_thumbprint' )
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 202) WITH NOWAIT;

							SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority,
							 FindingsGroup, Finding, Details)
								SELECT DISTINCT 202 AS CheckID,
								1 AS Priority,
								''Backup'' AS FindingsGroup,
								''Encryption Certificate Not Backed Up Recently'' AS Finding,
								''The certificate '' + c.name + '' is used to encrypt database backups. Last backup date: '' + COALESCE(CAST(c.pvt_key_last_backup_date AS VARCHAR(100)), ''Never'') AS Details
								FROM sys.certificates c
                                INNER JOIN msdb.dbo.backupset bs ON c.thumbprint = bs.encryptor_thumbprint
                                WHERE pvt_key_last_backup_date IS NULL OR pvt_key_last_backup_date <= DATEADD(dd, -30, GETDATE()) OPTION (RECOMPILE);';
							
							IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
							IF @Debug = 2 AND @StringToExecute IS NULL PRINT 
							'@StringToExecute has gone NULL, for some reason.';
							
							EXECUTE(@StringToExecute);
						END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 3 )
					BEGIN
						IF DATEADD(dd, -60, GETDATE()) > (SELECT TOP 1 backup_start_date FROM msdb.dbo.backupset ORDER BY backup_start_date)

						BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 3) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT TOP 1
										3 AS CheckID ,
										'msdb' ,
										200 AS Priority ,
										'Backup' AS FindingsGroup ,
										'MSDB Backup History Not Purged' AS Finding ,
										( 'Database backup history retained back to '
										  + CAST(bs.backup_start_date AS VARCHAR(20)) ) AS Details
								FROM    msdb.dbo.backupset bs
								ORDER BY backup_start_date ASC;
						END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 186 )
					BEGIN
						IF DATEADD(dd, -2, GETDATE()) < (SELECT TOP 1 backup_start_date FROM msdb.dbo.backupset ORDER BY backup_start_date)

						BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 186) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  DatabaseName ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT TOP 1
											186 AS CheckID ,
											'msdb' ,
											200 AS Priority ,
											'Backup' AS FindingsGroup ,
											'MSDB Backup History Purged Too Frequently' AS Finding ,
											( 'Database backup history only retained back to '
											  + CAST(bs.backup_start_date AS VARCHAR(20)) ) AS Details
									FROM    msdb.dbo.backupset bs
									ORDER BY backup_start_date ASC;
						END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 178 )
					AND EXISTS (SELECT *
									FROM msdb.dbo.backupset bs
									WHERE bs.type = 'D'
									AND bs.backup_size >= 50000000000 
									AND DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date) <= 60 
									AND bs.backup_finish_date >= DATEADD(DAY, -14, GETDATE()) )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 178) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT 178 AS CheckID ,
										200 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Snapshot Backups Occurring' AS Finding ,
										( CAST(COUNT(*) AS VARCHAR(20)) + ' snapshot-looking backups have occurred in the last two weeks, indicating that IO may be freezing up.') AS Details
								FROM msdb.dbo.backupset bs
								WHERE bs.type = 'D'
								AND bs.backup_size >= 50000000000 
								AND DATEDIFF(SECOND, bs.backup_start_date, bs.backup_finish_date) <= 60
								AND bs.backup_finish_date >= DATEADD(DAY, -14, GETDATE());
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 4 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 4) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  4 AS CheckID ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'Sysadmins' AS Finding ,
										( 'Login [' + l.name
										  + '] is a sysadmin - meaning they can do absolutely anything in SQL Server, including dropping databases or hiding their tracks.' ) AS Details
								FROM    master.sys.syslogins l
								WHERE   l.sysadmin = 1
										AND l.name <> SUSER_SNAME(0x01)
										AND l.denylogin = 0
										AND l.name NOT LIKE 'NT SERVICE\%'
										AND l.name <> 'l_certSignSmDetach'; 
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 5 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 5) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  5 AS CheckID ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'Security Admins' AS Finding ,
										('Login [' + l.name
										  + '] is a security admin - meaning they can give themselves permission to do
										   absolutely anything in SQL Server, including dropping databases or hiding their tracks.' ) AS Details
								FROM    master.sys.syslogins l
								WHERE   l.securityadmin = 1
										AND l.name <> SUSER_SNAME(0x01)
										AND l.denylogin = 0;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 104 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 104) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( [CheckID] ,
								  [Priority] ,
								  [FindingsGroup] ,
								  [Finding] ,
								  [Details]
								)
								SELECT  104 AS [CheckID] ,
										230 AS [Priority] ,
										'Security' AS [FindingsGroup] ,
										'Login Can Control Server' AS [Finding] ,
										'Login [' + pri.[name]
										+ '] has the CONTROL SERVER permission - meaning they can do absolutely anything in SQL Server, including dropping databases or hiding their tracks.' AS [Details]
								FROM    sys.server_principals AS pri
								WHERE   pri.[principal_id] IN (
										SELECT  p.[grantee_principal_id]
										FROM    sys.server_permissions AS p
										WHERE   p.[state] IN ( 'G', 'W' )
												AND p.[class] = 100
												AND p.[type] = 'CL' )
										AND pri.[name] NOT LIKE '##%##';
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 6 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 6) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  6 AS CheckID ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'Jobs Owned By Users' AS Finding ,
										('Job [' + j.name + '] is owned by ['
										  + SUSER_SNAME(j.owner_sid)
										  + '] - meaning if their login is disabled or not available due to Active Directory problems, the job will stop working.' ) AS Details
								FROM    msdb.dbo.sysjobs j
								WHERE   j.enabled = 1
										AND SUSER_SNAME(j.owner_sid) <> SUSER_SNAME(0x01);
					END;

				
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 7 )
					BEGIN
						
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 7) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  7 AS CheckID ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'Stored Procedure Runs at Startup' AS Finding ,
										( 'Stored procedure [master].['
										  + r.SPECIFIC_SCHEMA + '].['
										  + r.SPECIFIC_NAME
										  + '] runs automatically when SQL Server starts up.  Make sure you know exactly what this stored procedure is doing, because it could pose a security risk.' ) AS Details
								FROM    master.INFORMATION_SCHEMA.ROUTINES r
								WHERE   OBJECTPROPERTY(OBJECT_ID(ROUTINE_NAME),
													   'ExecIsStartup') = 1;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 10 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 10) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							Details)
					  SELECT 10 AS CheckID,
					  100 AS Priority,
					  ''Performance'' AS FindingsGroup,
					  ''Resource Governor Enabled'' AS Finding,
					  (''Resource Governor is enabled.  Queries may be throttled. '') AS Details FROM sys.resource_governor_configuration WHERE is_enabled = 1 OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 11 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 11) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							Details)
					  SELECT 11 AS CheckID,
					  100 AS Priority,
					  ''Performance'' AS FindingsGroup,
					  ''Server Triggers Enabled'' AS Finding,
					  (''Server Trigger ['' + [name] ++ ''] is enabled. '') AS Details FROM sys.server_triggers WHERE is_disabled = 0 AND is_ms_shipped = 0  OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 12 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 12) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  12 AS CheckID ,
										[name] AS DatabaseName ,
										10 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Close Enabled' AS Finding ,
										( 'Database [' + [name]
										  + '] has auto-close enabled.  This setting can dramatically decrease performance.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_close_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 12);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 13 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 13) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  13 AS CheckID ,
										[name] AS DatabaseName ,
										10 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Shrink Enabled' AS Finding ,
										( 'Database [' + [name]
										  + '] has auto-shrink enabled.  This setting can dramatically decrease performance.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_shrink_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 13);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 14 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 14) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
							(CheckID,
							DatabaseName,
							Priority,
							FindingsGroup,
							Finding,
							Details)
					  SELECT 14 AS CheckID,
					  [name] as DatabaseName,
					  50 AS Priority,
					  ''Reliability'' AS FindingsGroup,
					  ''Page Verification Not Optimal'' AS Finding,
					  (''Database ['' + [name] + ''] has '' + [page_verify_option_desc] + '' for page verification.  SQL Server may have a harder time recognizing and recovering from storage corruption.  Consider using CHECKSUM instead.'') COLLATE database_default AS Details
					  FROM sys.databases
					  WHERE page_verify_option < 2
					  AND name <> ''tempdb''
					  and name not in (select distinct DatabaseName from #SkipChecks WHERE CheckID IS NULL OR CheckID = 14) OPTION (RECOMPILE);';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 15 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 15) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  15 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Create Stats Disabled' AS Finding ,
										( 'Database [' + [name]
										  + '] has auto-create-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_create_stats_on = 0
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 15);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 16 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 16) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  16 AS CheckID ,
										[name] AS DatabaseName ,
										110 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Auto-Update Stats Disabled' AS Finding ,
										('Database [' + [name]
										  + '] has auto-update-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_update_stats_on = 0
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 16);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 17 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 17) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  17 AS CheckID ,
										[name] AS DatabaseName ,
										150 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Stats Updated Asynchronously' AS Finding ,
										('Database [' + [name]
										  + '] has auto-update-stats-async enabled' ) AS Details
								FROM    sys.databases
								WHERE   is_auto_update_stats_async_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 17);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 20 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 20) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  20 AS CheckID ,
										[name] AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Date Correlation On' AS Finding ,
										( 'Database [' + [name]
										  + '] has date correlation enabled.  This is not a default setting, and it has some performance overhead.  It tells SQL Server that date fields in two tables are related, and SQL Server maintains statistics showing that relation.' ) AS Details
								FROM    sys.databases
								WHERE   is_date_correlation_on = 1
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 20);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 21 )
					BEGIN
						
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 21) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
							(CheckID,
							DatabaseName,
							Priority,
							FindingsGroup,
							Finding,
							Details)
					  SELECT 21 AS CheckID,
					  [name] as DatabaseName,
					  200 AS Priority,
					  ''Informational'' AS FindingsGroup,
					  ''Database Encrypted'' AS Finding,
					  (''Database ['' + [name] + ''] has Transparent Data Encryption enabled.  Make absolutely sure you have backed up the certificate and private key, or else you will not be able to restore this database.'') AS Details
					  FROM sys.databases
					  WHERE is_encrypted = 1
					  and name not in (select distinct DatabaseName from #SkipChecks WHERE CheckID IS NULL OR CheckID = 21) OPTION (RECOMPILE);';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;
					END;

								IF @Debug IN (1, 2) RAISERROR('Generating default configuration values', 0, 1) WITH NOWAIT;

				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'access check cache bucket count', 0, 1001 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'access check cache quota', 0, 1002 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Ad Hoc Distributed Queries', 0, 1003 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity I/O mask', 0, 1004 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity mask', 0, 1005 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity64 mask', 0, 1066 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'affinity64 I/O mask', 0, 1067 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Agent XPs', 0, 1071 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'allow updates', 0, 1007 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'awe enabled', 0, 1008 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'backup checksum default', 0, 1070 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'backup compression default', 0, 1073 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'blocked process threshold', 0, 1009 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'blocked process threshold (s)', 0, 1009 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'c2 audit mode', 0, 1010 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'clr enabled', 0, 1011 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'common criteria compliance enabled', 0, 1074 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'contained database authentication', 0, 1068 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'cost threshold for parallelism', 5, 1012 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'cross db ownership chaining', 0, 1013 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'cursor threshold', -1, 1014 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Database Mail XPs', 0, 1072 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'default full-text language', 1033, 1016 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'default language', 0, 1017 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'default trace enabled', 1, 1018 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'disallow results from triggers', 0, 1019 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'EKM provider enabled', 0, 1075 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'filestream access level', 0, 1076 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'fill factor (%)', 0, 1020 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft crawl bandwidth (max)', 100, 1021 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft crawl bandwidth (min)', 0, 1022 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft notify bandwidth (max)', 100, 1023 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'ft notify bandwidth (min)', 0, 1024 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'index create memory (KB)', 0, 1025 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'in-doubt xact resolution', 0, 1026 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'lightweight pooling', 0, 1027 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'locks', 0, 1028 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max degree of parallelism', 0, 1029 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max full-text crawl range', 4, 1030 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max server memory (MB)', 2147483647, 1031 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max text repl size (B)', 65536, 1032 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'max worker threads', 0, 1033 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'media retention', 0, 1034 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'min memory per query (KB)', 1024, 1035 );
								IF EXISTS ( SELECT  *
							FROM    sys.configurations
							WHERE   name = 'min server memory (MB)'
									AND value_in_use IN ( 0, 16 ) )
					INSERT  INTO #ConfigurationDefaults
							SELECT  'min server memory (MB)' ,
									CAST(value_in_use AS BIGINT), 1036
							FROM    sys.configurations
							WHERE   name = 'min server memory (MB)';
				ELSE
					INSERT  INTO #ConfigurationDefaults
					VALUES  ( 'min server memory (MB)', 0, 1036 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'nested triggers', 1, 1037 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'network packet size (B)', 4096, 1038 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Ole Automation Procedures', 0, 1039 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'open objects', 0, 1040 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'optimize for ad hoc workloads', 0, 1041 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'PH timeout (s)', 60, 1042 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'precompute rank', 0, 1043 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'priority boost', 0, 1044 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'query governor cost limit', 0, 1045 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'query wait (s)', -1, 1046 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'recovery interval (min)', 0, 1047 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote access', 1, 1048 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote admin connections', 0, 1049 );
				
				IF @@VERSION LIKE '%Microsoft SQL Server 2005%'
					OR @@VERSION LIKE '%Microsoft SQL Server 2008%'
					BEGIN
						INSERT  INTO #ConfigurationDefaults
						VALUES  ( 'remote login timeout (s)', 20, 1069 );
					END;
				ELSE
					BEGIN
						INSERT  INTO #ConfigurationDefaults
						VALUES  ( 'remote login timeout (s)', 10, 1069 );
					END;
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote proc trans', 0, 1050 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'remote query timeout (s)', 600, 1051 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Replication XPs', 0, 1052 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'RPC parameter data validation', 0, 1053 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'scan for startup procs', 0, 1054 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'server trigger recursion', 1, 1055 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'set working set size', 0, 1056 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'show advanced options', 0, 1057 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'SMO and DMO XPs', 1, 1058 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'SQL Mail XPs', 0, 1059 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'transform noise words', 0, 1060 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'two digit year cutoff', 2049, 1061 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'user connections', 0, 1062 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'user options', 0, 1063 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'Web Assistant Procedures', 0, 1064 );
				INSERT  INTO #ConfigurationDefaults
				VALUES  ( 'xp_cmdshell', 0, 1065 );

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 22 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 22) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  cd.CheckID ,
										200 AS Priority ,
										'Non-Default Server Config' AS FindingsGroup ,
										cr.name AS Finding ,
										( 'This sp_configure option has been changed.  Its default value is '
										  + COALESCE(CAST(cd.[DefaultValue] AS VARCHAR(100)),
													 '(unknown)')
										  + ' and it has been set to '
										  + CAST(cr.value_in_use AS VARCHAR(100))
										  + '.' ) AS Details
								FROM    sys.configurations cr
										INNER JOIN #ConfigurationDefaults cd ON cd.name = cr.name
										LEFT OUTER JOIN #ConfigurationDefaults cdUsed ON cdUsed.name = cr.name
																  AND cdUsed.DefaultValue = cr.value_in_use
								WHERE   cdUsed.name IS NULL;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 190 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Setting @MinServerMemory and @MaxServerMemory', 0, 1) WITH NOWAIT;

						SELECT @MinServerMemory = CAST(value_in_use as BIGINT) FROM sys.configurations WHERE name = 'min server memory (MB)';
						SELECT @MaxServerMemory = CAST(value_in_use as BIGINT) FROM sys.configurations WHERE name = 'max server memory (MB)';
						
						IF (@MinServerMemory = @MaxServerMemory)
						BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 190) WITH NOWAIT;

						INSERT INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								VALUES
									(	190,
										200,
										'Performance',
										'Non-Dynamic Memory',
										'Minimum Server Memory setting is the same as the Maximum (both set to ' + CAST(@MinServerMemory AS NVARCHAR(50)) + '). This will not allow dynamic memory. Please revise memory settings'
									);
						END;
					END;
					
					IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 188 )
					BEGIN

						
						
						IF @Debug IN (1, 2) RAISERROR('Setting @Processors.', 0, 1) WITH NOWAIT;
						
						SET @Processors = (SELECT cpu_count FROM sys.dm_os_sys_info);
						
						IF @Debug IN (1, 2) RAISERROR('Setting @NUMANodes', 0, 1) WITH NOWAIT;
						
						SET @NUMANodes = (SELECT COUNT(1)
											FROM sys.dm_os_performance_counters pc
											WHERE pc.object_name LIKE '%Buffer Node%'
												AND counter_name = 'Page life expectancy');
						
						
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 188) WITH NOWAIT;
						
						INSERT INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  188 AS CheckID ,
										200 AS Priority ,
										'Performance' AS FindingsGroup ,
										cr.name AS Finding ,
										( 'Set to ' + CAST(cr.value_in_use AS NVARCHAR(50)) + ', its default value. Changing this sp_configure setting may reduce CXPACKET waits.')
								FROM    sys.configurations cr
										INNER JOIN #ConfigurationDefaults cd ON cd.name = cr.name
											AND cr.value_in_use = cd.DefaultValue
								WHERE   cr.name = 'cost threshold for parallelism'
									OR (cr.name = 'max degree of parallelism' AND (@NUMANodes > 1 OR @Processors > 8));
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 24 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 24) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT DISTINCT
										24 AS CheckID ,
										DB_NAME(database_id) AS DatabaseName ,
										170 AS Priority ,
										'File Configuration' AS FindingsGroup ,
										'System Database on C Drive' AS Finding ,
										( 'The ' + DB_NAME(database_id)
										  + ' database has a file on the C drive.  Putting system databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
								FROM    sys.master_files
								WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
										AND DB_NAME(database_id) IN ( 'master',
																  'model', 'msdb' );
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 25 )
                    AND SERVERPROPERTY('EngineEdition') <> 8 
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 25) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT TOP 1
										25 AS CheckID ,
										'tempdb' ,
										20 AS Priority ,
										'File Configuration' AS FindingsGroup ,
										'TempDB on C Drive' AS Finding ,
										CASE WHEN growth > 0
											 THEN ( 'The tempdb database has files on the C drive.  TempDB frequently grows unpredictably, putting your server at risk of running out of C drive space and crashing hard.  C is also often much slower than other drives, so performance may be suffering.' )
											 ELSE ( 'The tempdb database has files on the C drive.  TempDB is not set to Autogrow, hopefully it is big enough.  C is also often much slower than other drives, so performance may be suffering.' )
										END AS Details
								FROM    sys.master_files
								WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
										AND DB_NAME(database_id) = 'tempdb';
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 26 )
                    AND SERVERPROPERTY('EngineEdition') <> 8 
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 26) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT DISTINCT
										26 AS CheckID ,
										DB_NAME(database_id) AS DatabaseName ,
										20 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'User Databases on C Drive' AS Finding ,
										( 'The ' + DB_NAME(database_id)
										  + ' database has a file on the C drive.  Putting databases on the C drive runs the risk of crashing the server when it runs out of space.' ) AS Details
								FROM    sys.master_files
								WHERE   UPPER(LEFT(physical_name, 1)) = 'C'
										AND DB_NAME(database_id) NOT IN ( 'master',
																  'model', 'msdb',
																  'tempdb' )
										AND DB_NAME(database_id) NOT IN (
										SELECT DISTINCT
												DatabaseName
										FROM    #SkipChecks
										WHERE CheckID IS NULL OR CheckID = 26 );
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 27 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 27) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  27 AS CheckID ,
										'master' AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Tables in the Master Database' AS Finding ,
										( 'The ' + name
										  + ' table in the master database was created by end users on '
										  + CAST(create_date AS VARCHAR(20))
										  + '. Tables in the master database may not be restored in the event of a disaster.' ) AS Details
								FROM    master.sys.tables
								WHERE   is_ms_shipped = 0
                                  AND   name NOT IN ('CommandLog','SqlServerVersions');
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 28 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 28) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  28 AS CheckID ,
								        'msdb' AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Tables in the MSDB Database' AS Finding ,
										( 'The ' + name
										  + ' table in the msdb database was created by end users on '
										  + CAST(create_date AS VARCHAR(20))
										  + '. Tables in the msdb database may not be restored in the event of a disaster.' ) AS Details
								FROM    msdb.sys.tables
								WHERE   is_ms_shipped = 0 AND name NOT LIKE '%DTA_%';
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 29 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 29) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  29 AS CheckID ,
								        'model' AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Tables in the Model Database' AS Finding ,
										( 'The ' + name
										  + ' table in the model database was created by end users on '
										  + CAST(create_date AS VARCHAR(20))
										  + '. Tables in the model database are automatically copied into all new databases.' ) AS Details
								FROM    model.sys.tables
								WHERE   is_ms_shipped = 0;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 30 )
					BEGIN
						IF ( SELECT COUNT(*)
							 FROM   msdb.dbo.sysalerts
							 WHERE  severity BETWEEN 19 AND 25
						   ) < 7

						   BEGIN

						   IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 30) WITH NOWAIT;

							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  30 AS CheckID ,
											200 AS Priority ,
											'Monitoring' AS FindingsGroup ,
											'Not All Alerts Configured' AS Finding ,
											( 'Not all SQL Server Agent alerts have been configured.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.' ) AS Details;
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 59 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    msdb.dbo.sysalerts
									WHERE   enabled = 1
											AND COALESCE(has_notification, 0) = 0
											AND (job_id IS NULL OR job_id = 0x))

							BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 59) WITH NOWAIT;

							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  59 AS CheckID ,
											200 AS Priority ,
											'Monitoring' AS FindingsGroup ,
											'Alerts Configured without Follow Up' AS Finding ,
											( 'SQL Server Agent alerts have been configured but they either do not notify anyone or else they do not take any action.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.' ) AS Details;
					
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 96 )
					BEGIN
						IF NOT EXISTS ( SELECT  *
										FROM    msdb.dbo.sysalerts
										WHERE   message_id IN ( 823, 824, 825 ) )
							
							BEGIN;
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 96) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  96 AS CheckID ,
											200 AS Priority ,
											'Monitoring' AS FindingsGroup ,
											'No Alerts for Corruption' AS Finding ,
											( 'SQL Server Agent alerts do not exist for errors 823, 824, and 825.  These three errors can give you notification about early hardware failure. Enabling them can prevent you a lot of heartbreak.' ) AS Details;
					
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 61 )
					BEGIN
						IF NOT EXISTS ( SELECT  *
										FROM    msdb.dbo.sysalerts
										WHERE   severity BETWEEN 19 AND 25 )
							
							BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 61) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  61 AS CheckID ,
											200 AS Priority ,
											'Monitoring' AS FindingsGroup ,
											'No Alerts for Sev 19-25' AS Finding ,
											( 'SQL Server Agent alerts do not exist for severity levels 19 through 25.  These are some very severe SQL Server errors. Knowing that these are happening may let you recover from errors faster.' ) AS Details;
					
							END;

					END;

		--check for disabled alerts
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 98 )
					BEGIN
						IF EXISTS ( SELECT  name
									FROM    msdb.dbo.sysalerts
									WHERE   enabled = 0 )
							
							BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 98) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  98 AS CheckID ,
											200 AS Priority ,
											'Monitoring' AS FindingsGroup ,
											'Alerts Disabled' AS Finding ,
											( 'The following Alert is disabled, please review and enable if desired: '
											  + name ) AS Details
									FROM    msdb.dbo.sysalerts
									WHERE   enabled = 0;
					
							END;
					
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 31 )
					BEGIN
						IF NOT EXISTS ( SELECT  *
										FROM    msdb.dbo.sysoperators
										WHERE   enabled = 1 )
							
							BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 31) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  31 AS CheckID ,
											200 AS Priority ,
											'Monitoring' AS FindingsGroup ,
											'No Operators Configured/Enabled' AS Finding ,
											( 'No SQL Server Agent operators (emails) have been configured.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.' ) AS Details;
					
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 34 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    sys.all_objects
									WHERE   name = 'dm_db_mirroring_auto_page_repair' )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 34) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT DISTINCT
		  34 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ( ''Database mirroring has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_db_mirroring_auto_page_repair.'' ) AS Details
		  FROM (SELECT rp2.database_id, rp2.modification_time
			FROM sys.dm_db_mirroring_auto_page_repair rp2
			WHERE rp2.[database_id] not in (
			SELECT db2.[database_id]
			FROM sys.databases as db2
			WHERE db2.[state] = 1
			) ) as rp
		  INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
		  WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE())  OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 89 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    sys.all_objects
									WHERE   name = 'dm_hadr_auto_page_repair' )
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 89) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT DISTINCT
		  89 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ( ''Availability Groups has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_hadr_auto_page_repair.'' ) AS Details
		  FROM    sys.dm_hadr_auto_page_repair rp
		  INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
		  WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE()) OPTION (RECOMPILE) ;';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 90 )
					BEGIN
						IF EXISTS ( SELECT  *
									FROM    msdb.sys.all_objects
									WHERE   name = 'suspect_pages' )
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 90) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT DISTINCT
		  90 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ( ''SQL Server has detected at least one corrupt page in the last 30 days. 
		  For more information, query the system table msdb.dbo.suspect_pages.'' ) AS Details
		  FROM    msdb.dbo.suspect_pages sp
		  INNER JOIN master.sys.databases db ON sp.database_id = db.database_id
		  WHERE   sp.last_update_date >= DATEADD(dd, -30, GETDATE())  OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 36 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 36) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT DISTINCT
										36 AS CheckID ,
										150 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Slow Storage Reads on Drive '
										+ UPPER(LEFT(mf.physical_name, 1)) AS Finding ,
										'Reads are averaging longer than 200ms for at least one database on this drive.
										  For specific database file speeds, run the query from the information link.' AS Details
								FROM    sys.dm_io_virtual_file_stats(NULL, NULL)
										AS fs
										INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id
																  AND fs.[file_id] = mf.[file_id]
								WHERE   ( io_stall_read_ms / ( 1.0 + num_of_reads ) ) > 200
								AND num_of_reads > 100000;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 37 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 37) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT DISTINCT
										37 AS CheckID ,
										150 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Slow Storage Writes on Drive '
										+ UPPER(LEFT(mf.physical_name, 1)) AS Finding ,
										'Writes are averaging longer than 100ms for at least one database on this drive.
										  For specific database file speeds, run the query from the information link.' AS Details
								FROM    sys.dm_io_virtual_file_stats(NULL, NULL)
										AS fs
										INNER JOIN sys.master_files AS mf ON fs.database_id = mf.database_id
																  AND fs.[file_id] = mf.[file_id]
								WHERE   ( io_stall_write_ms / ( 1.0
																+ num_of_writes ) ) > 100
																AND num_of_writes > 100000;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 40 )
					BEGIN
						IF ( SELECT COUNT(*)
							 FROM   tempdb.sys.database_files
							 WHERE  type_desc = 'ROWS'
						   ) = 1
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 40) WITH NOWAIT;

								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  DatabaseName ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
								VALUES  ( 40 ,
										  'tempdb' ,
										  170 ,
										  'File Configuration' ,
										  'TempDB Only Has 1 Data File' ,
										  'TempDB is only configured with one data file.  
										  More data files are usually required to alleviate SGAM contention.'
										);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 183 )

				BEGIN

						IF ( SELECT COUNT (distinct [size])
							FROM   tempdb.sys.database_files
							WHERE  type_desc = 'ROWS'
							HAVING MAX((size * 8) / (1024. * 1024)) - MIN((size * 8) / (1024. * 1024)) > 1.
							) <> 1
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 183) WITH NOWAIT;

								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  DatabaseName ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
								VALUES  ( 183 ,
										  'tempdb' ,
										  170 ,
										  'File Configuration' ,
										  'TempDB Unevenly Sized Data Files' ,
										  'TempDB data files are not configured with the same size. 
										   Unevenly sized tempdb data files will result in unevenly sized workloads.'
										);
							END;
				END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 44 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 44) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  44 AS CheckID ,
										150 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Queries Forcing Order Hints' AS Finding ,
										CAST(occurrence AS VARCHAR(10))
										+ ' instances of order hinting have been recorded since restart. 
										 This means queries are bossing the SQL Server optimizer around, and if they don''t know what they''re doing, this can cause more harm than good.  This can also explain why DBA tuning efforts aren''t working.' AS Details
								FROM    sys.dm_exec_query_optimizer_info
								WHERE   counter = 'order hint'
										AND occurrence > 1000;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 45 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 45) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  45 AS CheckID ,
										150 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Queries Forcing Join Hints' AS Finding ,
										CAST(occurrence AS VARCHAR(10))
										+ ' instances of join hinting have been recorded since restart. 
										 This means queries are bossing the SQL Server optimizer around,
										  and if they don''t know what they''re doing, this can cause more harm than good.  This can also explain why DBA tuning efforts aren''t working.' AS Details
								FROM    sys.dm_exec_query_optimizer_info
								WHERE   counter = 'join hint'
										AND occurrence > 1000;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 49 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 49) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT DISTINCT
										49 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Linked Server Configured' AS Finding ,
										+CASE WHEN l.remote_name = 'sa'
											  THEN COALESCE(s.data_source, s.provider)
												   + ' is configured as a linked server. Check its security configuration as it is connecting with sa, because any user who queries it will get admin-level permissions.'
											  ELSE COALESCE(s.data_source, s.provider)
												   + ' is configured as a linked server. Check its security configuration to make sure it isn''t connecting with SA or some other bone-headed administrative login, because any user who queries it might get admin-level permissions.'
										 END AS Details
								FROM    sys.servers s
										INNER JOIN sys.linked_logins l ON s.server_id = l.server_id
								WHERE   s.is_linked = 1;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 50 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 50) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults 
								(CheckID, Priority, FindingsGroup, Finding, Details)
		  SELECT  50 AS CheckID ,
		  100 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Max Memory Set Too High'' AS Finding ,
		  ''SQL Server max memory is set to ''
			+ CAST(c.value_in_use AS VARCHAR(20))
			+ '' megabytes, but the server only has ''
			+ CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
			+ '' megabytes.  SQL Server may drain the system dry of memory, and under certain conditions, 
			this can cause Windows to swap to disk.'' AS Details
		  FROM    sys.dm_os_sys_memory m
		  INNER JOIN sys.configurations c ON c.name = ''max server memory (MB)''
		  WHERE   CAST(m.total_physical_memory_kb AS BIGINT) < ( CAST(c.value_in_use AS BIGINT) * 1024 ) OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 51 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 51) WITH NOWAIT

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults 
								(CheckID, Priority, FindingsGroup, Finding, Details)
		  SELECT  51 AS CheckID ,
		  1 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Memory Dangerously Low'' AS Finding ,
		  ''The server has '' + CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20)) + '' megabytes of physical memory, but only '' + CAST(( CAST(m.available_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
			+ '' megabytes are available.  As the server runs out of memory, there is danger of swapping to disk, which will kill performance.'' AS Details
		  FROM    sys.dm_os_sys_memory m
		  WHERE   CAST(m.available_physical_memory_kb AS BIGINT) < 262144 OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 159 )
					BEGIN
						IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 159) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults 
								(CheckID, Priority, FindingsGroup, Finding, Details)
		  SELECT DISTINCT 159 AS CheckID ,
		  1 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Memory Dangerously Low in NUMA Nodes'' AS Finding ,
		  ''At least one NUMA node is reporting THREAD_RESOURCES_LOW in sys.dm_os_nodes and can no longer create threads.'' AS Details
		  FROM    sys.dm_os_nodes m
		  WHERE   node_state_desc LIKE ''%THREAD_RESOURCES_LOW%'' OPTION (RECOMPILE);';

								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

								EXECUTE(@StringToExecute);
							END;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 53 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 53) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT TOP 1
										53 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Cluster Node' AS Finding ,
										'This is a node in a cluster.' AS Details
								FROM    sys.dm_os_cluster_nodes;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 55 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 55) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  55 AS CheckID ,
										[name] AS DatabaseName ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'Database Owner <> SA' AS Finding ,
										( 'Database name: ' + [name] + '   '
										  + 'Owner name: ' + SUSER_SNAME(owner_sid) ) AS Details
								FROM    sys.databases
								WHERE   SUSER_SNAME(owner_sid) <> SUSER_SNAME(0x01)
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 55);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 213 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 213) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  213 AS CheckID ,
										[name] AS DatabaseName ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'Database Owner is Unknown' AS Finding ,
										( 'Database name: ' + [name] + '   '
										  + 'Owner name: ' + ISNULL(SUSER_SNAME(owner_sid),'~~ UNKNOWN ~~') ) AS Details
								FROM    sys.databases
								WHERE   SUSER_SNAME(owner_sid) is NULL
										AND name NOT IN ( SELECT DISTINCT DatabaseName
														  FROM    #SkipChecks 
														  WHERE CheckID IS NULL OR CheckID = 213);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 57 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 57) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  57 AS CheckID ,
										230 AS Priority ,
										'Security' AS FindingsGroup ,
										'SQL Agent Job Runs at Startup' AS Finding ,
										( 'Job [' + j.name
										  + '] runs automatically when SQL Server Agent starts up.
										    Make sure you know exactly what this job is doing, 
										    because it could pose a security risk.' ) AS Details
								FROM    msdb.dbo.sysschedules sched
										JOIN msdb.dbo.sysjobschedules jsched ON sched.schedule_id = jsched.schedule_id
										JOIN msdb.dbo.sysjobs j ON jsched.job_id = j.job_id
								WHERE   sched.freq_type = 64
								        AND sched.enabled = 1;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 97 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 97) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  97 AS CheckID ,
										100 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Unusual SQL Server Edition' AS Finding ,
										( 'This server is using '
										  + CAST(SERVERPROPERTY('edition') AS VARCHAR(100))
										  + ', which is capped at low amounts of CPU and memory.' ) AS Details
								WHERE   CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Standard%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Enterprise%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Data Center%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Developer%'
										AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Business Intelligence%';
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 154 )
                    AND SERVERPROPERTY('EngineEdition') <> 8
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 154) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  154 AS CheckID ,
										10 AS Priority ,
										'Performance' AS FindingsGroup ,
										'32-bit SQL Server Installed' AS Finding ,
										( 'This server uses the 32-bit x86 binaries for SQL Server instead 
										of the 64-bit x64 binaries. The amount of memory available for query workspace 
										and execution plans is heavily limited.' ) AS Details
								WHERE   CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%64%';
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 62 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 62) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  62 AS CheckID ,
										[name] AS DatabaseName ,
										200 AS Priority ,
										'Performance' AS FindingsGroup ,
										'Old Compatibility Level' AS Finding ,
										( 'Database ' + [name]
										  + ' is compatibility level '
										  + CAST(compatibility_level AS VARCHAR(20))
										  + ', which may cause unwanted results when trying to run queries that
										   have newer T-SQL features.' ) AS Details
								FROM    sys.databases
								WHERE   name NOT IN ( SELECT DISTINCT
																DatabaseName
													  FROM      #SkipChecks
													  WHERE CheckID IS NULL OR CheckID = 62)
										AND compatibility_level <= 90;
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 94 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 94) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  94 AS CheckID ,
										200 AS [Priority] ,
										'Monitoring' AS FindingsGroup ,
										'Agent Jobs Without Failure Emails' AS Finding ,
										'The job ' + [name]
										+ ' has not been set up to notify an operator if it fails.' AS Details
								FROM    msdb.[dbo].[sysjobs] j
										INNER JOIN ( SELECT DISTINCT
															[job_id]
													 FROM   [msdb].[dbo].[sysjobschedules]
													 WHERE  next_run_date > 0
												   ) s ON j.job_id = s.job_id
								WHERE   j.enabled = 1
										AND j.notify_email_operator_id = 0
										AND j.notify_netsend_operator_id = 0
										AND j.notify_page_operator_id = 0
										AND j.category_id <> 100; 
					END;

				IF EXISTS ( SELECT  1
							FROM    sys.configurations
							WHERE   name = 'remote admin connections'
									AND value_in_use = 0 )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 100 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 100) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  100 AS CheckID ,
										50 AS Priority ,
										'Reliability' AS FindingGroup ,
										'Remote DAC Disabled' AS Finding ,
										'Remote access to the Dedicated Admin Connection (DAC) is not enabled. 
										The DAC can make remote troubleshooting much easier when SQL Server is unresponsive.';
					END;

				IF EXISTS ( SELECT  *
							FROM    sys.dm_os_schedulers
							WHERE   is_online = 0 )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 101 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 101) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  101 AS CheckID ,
										50 AS Priority ,
										'Performance' AS FindingGroup ,
										'CPU Schedulers Offline' AS Finding ,
										'Some CPU cores are not accessible to SQL Server due to affinity masking 
										or licensing problems.';
					END;

					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 110 )
								AND EXISTS (SELECT * FROM master.sys.all_objects WHERE name = 'dm_os_memory_nodes')
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 110) WITH NOWAIT;

							SET @StringToExecute = 'IF EXISTS (SELECT  *
												FROM sys.dm_os_nodes n
												INNER JOIN sys.dm_os_memory_nodes m ON n.memory_node_id = m.memory_node_id
												WHERE n.node_state_desc = ''OFFLINE'')
												INSERT  INTO #SQLCheckResults
														( CheckID ,
														  Priority ,
														  FindingsGroup ,
														  Finding ,
														  Details
														)
														SELECT  110 AS CheckID ,
																50 AS Priority ,
																''Performance'' AS FindingGroup ,
																''Memory Nodes Offline'' AS Finding ,
																''Due to affinity masking or licensing problems,
																 some of the memory may not be available.'' OPTION (RECOMPILE)';
									
									IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
									IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
									
									EXECUTE(@StringToExecute);
						END;

				IF EXISTS ( SELECT  *
							FROM    sys.databases
							WHERE   state > 1 )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 102 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 102) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  102 AS CheckID ,
										[name] ,
										20 AS Priority ,
										'Reliability' AS FindingGroup ,
										'Unusual Database State: ' + [state_desc] AS Finding ,
										'This database may not be online.'
								FROM    sys.databases
								WHERE   state > 1;
					END;

				IF EXISTS ( SELECT  *
							FROM    master.sys.extended_procedures )
					AND NOT EXISTS ( SELECT 1
									 FROM   #SkipChecks
									 WHERE  DatabaseName IS NULL AND CheckID = 105 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 105) WITH NOWAIT;

						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  105 AS CheckID ,
										'master' ,
										200 AS Priority ,
										'Reliability' AS FindingGroup ,
										'Extended Stored Procedures in Master' AS Finding ,
										'The [' + name
										+ '] extended stored procedure is in the master database. 
										CLR may be in use, and the master database now needs to be part of 
										your backup/recovery planning.'
								FROM    master.sys.extended_procedures;
					END;

					IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 107 )
						BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 107) WITH NOWAIT;

							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  107 AS CheckID ,
											50 AS Priority ,
											'Performance' AS FindingGroup ,
											'Poison Wait Detected: ' + wait_type  AS Finding ,
											CONVERT(VARCHAR(10), (SUM([wait_time_ms]) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM([wait_time_ms]) / 1000), 0), 108) + ' of this wait have been recorded. This wait often indicates killer performance problems.'
									FROM sys.[dm_os_wait_stats]
									WHERE wait_type IN('IO_QUEUE_LIMIT', 'IO_RETRY', 'LOG_RATE_GOVERNOR', 'PREEMPTIVE_DEBUG', 'RESMGR_THROTTLED', 'RESOURCE_SEMAPHORE', 'RESOURCE_SEMAPHORE_QUERY_COMPILE','SE_REPL_CATCHUP_THROTTLE','SE_REPL_COMMIT_ACK','SE_REPL_COMMIT_TURN','SE_REPL_ROLLBACK_ACK','SE_REPL_SLOW_SECONDARY_THROTTLE','THREADPOOL')
									GROUP BY wait_type
								    HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
									AND SUM([wait_time_ms]) > 60000;
						END;

					IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 121 )
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 121) WITH NOWAIT;

							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  121 AS CheckID ,
											50 AS Priority ,
											'Performance' AS FindingGroup ,
											'Poison Wait Detected: Serializable Locking'  AS Finding ,
											CONVERT(VARCHAR(10), (SUM([wait_time_ms]) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (SUM([wait_time_ms]) / 1000), 0), 108) + ' of LCK_M_R% waits have been recorded. This wait often indicates killer performance problems.'
									FROM sys.[dm_os_wait_stats]
									WHERE wait_type IN ('LCK_M_RS_S', 'LCK_M_RS_U', 'LCK_M_RIn_NL','LCK_M_RIn_S', 'LCK_M_RIn_U','LCK_M_RIn_X', 'LCK_M_RX_S', 'LCK_M_RX_U','LCK_M_RX_X')
								    HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
									AND SUM([wait_time_ms]) > 60000;
						END;


						IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 111 )
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 111) WITH NOWAIT;

							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  DatabaseName ,
									  Details
									)
									SELECT  111 AS CheckID ,
											50 AS Priority ,
											'Reliability' AS FindingGroup ,
											'Possibly Broken Log Shipping'  AS Finding ,
											d.[name] ,
											d.[name] + ' is in a restoring state, but has not had a backup applied in the last two days. This is a possible indication of a broken transaction log shipping setup.'
											FROM [master].sys.databases d
											INNER JOIN [master].sys.database_mirroring dm ON d.database_id = dm.database_id
												AND dm.mirroring_role IS NULL
											WHERE ( d.[state] = 1
											OR (d.[state] = 0 AND d.[is_in_standby] = 1) )
											AND NOT EXISTS(SELECT * FROM msdb.dbo.restorehistory rh
											INNER JOIN msdb.dbo.backupset bs ON rh.backup_set_id = bs.backup_set_id
											WHERE d.[name] COLLATE SQL_Latin1_General_CP1_CI_AS = rh.destination_database_name COLLATE SQL_Latin1_General_CP1_CI_AS
											AND rh.restore_date >= DATEADD(dd, -2, GETDATE()));

						END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 112 )
									AND EXISTS (SELECT * FROM master.sys.all_objects WHERE name = 'change_tracking_databases')
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 112) WITH NOWAIT;

								SET @StringToExecute = 'INSERT INTO #SQLCheckResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									Details)
							  SELECT 112 AS CheckID,
							  100 AS Priority,
							  ''Performance'' AS FindingsGroup,
							  ''Change Tracking Enabled'' AS Finding,
							  ( d.[name] + '' has change tracking enabled. This is not a default setting, 
							  and it has some performance overhead. It keeps track of changes to rows in tables that
							   have change tracking turned on.'' ) AS Details FROM sys.change_tracking_databases AS ctd INNER JOIN sys.databases AS d ON ctd.database_id = d.database_id OPTION (RECOMPILE)';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
							END;

						IF NOT EXISTS ( SELECT 1
										 FROM   #SkipChecks
										 WHERE  DatabaseName IS NULL AND CheckID = 116 )
									AND EXISTS (SELECT * FROM msdb.sys.all_columns WHERE name = 'compressed_backup_size')
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 116) WITH NOWAIT

							SET @StringToExecute = 'INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  116 AS CheckID ,
											200 AS Priority ,
											''Informational'' AS FindingGroup ,
											''Backup Compression Default Off''  AS Finding ,
											''Uncompressed full backups have happened recently, and backup compression is not turned on at the server level. Backup compression is included with SQL Server 2008R2 & newer, even in Standard Edition. We recommend turning backup compression on by default so that ad-hoc backups will get compressed.''
											FROM sys.configurations
											WHERE configuration_id = 1579 AND CAST(value_in_use AS INT) = 0
                                            AND EXISTS (SELECT * FROM msdb.dbo.backupset WHERE backup_size = compressed_backup_size AND type = ''D'' AND backup_finish_date >= DATEADD(DD, -14, GETDATE())) OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
						END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 117 )
									AND EXISTS (SELECT * FROM master.sys.all_objects WHERE name = 'dm_exec_query_resource_semaphores')
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 117) WITH NOWAIT;
								
								SET @StringToExecute = 'IF 0 < (SELECT SUM([forced_grant_count]) FROM sys.dm_exec_query_resource_semaphores WHERE [forced_grant_count] IS NOT NULL)
								INSERT INTO #SQLCheckResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									Details)
							  SELECT 117 AS CheckID,
							  100 AS Priority,
							  ''Performance'' AS FindingsGroup,
							  ''Memory Pressure Affecting Queries'' AS Finding,
							  CAST(SUM(forced_grant_count) AS NVARCHAR(100)) + '' forced grants reported in the DMV sys.dm_exec_query_resource_semaphores, indicating memory pressure has affected query runtimes.''
							  FROM sys.dm_exec_query_resource_semaphores WHERE [forced_grant_count] IS NOT NULL OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 124 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 124) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									Details)
								SELECT 124, 
										150, 
										'Performance', 
										'Deadlocks Happening Daily', 
										CAST(CAST(p.cntr_value / @DaysUptime AS BIGINT) AS NVARCHAR(100)) + ' average deadlocks per day' AS Details
								FROM sys.dm_os_performance_counters p
									INNER JOIN sys.databases d ON d.name = 'tempdb'
								WHERE RTRIM(p.counter_name) = 'Number of Deadlocks/sec'
									AND RTRIM(p.instance_name) = '_Total'
									AND p.cntr_value > 0
									AND (1.0 * p.cntr_value / NULLIF(datediff(DD,create_date,CURRENT_TIMESTAMP),0)) > 10;
							END;

						IF DATEADD(mi, -15, GETDATE()) < (SELECT TOP 1 creation_time FROM sys.dm_exec_query_stats ORDER BY creation_time)
						AND NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 125 )
						BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 125) WITH NOWAIT;
							
								DECLARE @user_perm_sql NVARCHAR(MAX) = N'';
								DECLARE @user_perm_gb_out DECIMAL(38,2);
								
								IF @ProductVersionMajor >= 11
								
								BEGIN
								
								SET @user_perm_sql += N'
									SELECT @user_perm_gb = CASE WHEN (pages_kb / 128.0 / 1024.) >= 2.
											THEN CONVERT(DECIMAL(38, 2), (pages_kb / 128.0 / 1024.))
											ELSE NULL 
										   END
									FROM sys.dm_os_memory_clerks
									WHERE type = ''USERSTORE_TOKENPERM''
									AND    name = ''TokenAndPermUserStore''
								';
								
								END
								
								IF @ProductVersionMajor < 11
								
								BEGIN
								SET @user_perm_sql += N'
									SELECT @user_perm_gb = CASE WHEN ((single_pages_kb + multi_pages_kb) / 1024.0 / 1024.) >= 2.
											THEN CONVERT(DECIMAL(38, 2), ((single_pages_kb + multi_pages_kb)  / 1024.0 / 1024.))
											ELSE NULL 
										   END
									FROM sys.dm_os_memory_clerks
									WHERE type = ''USERSTORE_TOKENPERM''
									AND    name = ''TokenAndPermUserStore''
								';
								
								END

								EXEC sys.sp_executesql @user_perm_sql, 
								                       N'@user_perm_gb DECIMAL(38,2) OUTPUT', 
													   @user_perm_gb = @user_perm_gb_out OUTPUT

							INSERT INTO #SQLCheckResults
								(CheckID,
								Priority,
								FindingsGroup,
								Finding,
								Details)
							SELECT TOP 1 125, 10, 'Performance', 'Plan Cache Erased Recently', 'The oldest query in the plan cache was created at ' + CAST(creation_time AS NVARCHAR(50)) 
								+ CASE WHEN @user_perm_gb_out IS NULL 
								       THEN '. Someone ran DBCC FREEPROCCACHE, restarted SQL Server, or it is under horrific memory pressure.'
									   ELSE '. You also have ' + CONVERT(NVARCHAR(20), @user_perm_gb_out) + ' GB of USERSTORE_TOKENPERM, which could indicate unusual memory consumption.'
								  END
							FROM sys.dm_exec_query_stats WITH (NOLOCK)
							ORDER BY creation_time;	
						END;

						IF EXISTS (SELECT * FROM sys.configurations WHERE name = 'priority boost' AND (value = 1 OR value_in_use = 1))
						AND NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 126 )
						BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 126) WITH NOWAIT;
							
							INSERT INTO #SQLCheckResults
								(CheckID,
								Priority,
								FindingsGroup,
								Finding,
								Details)
							VALUES(126, 5, 'Reliability', 'Priority Boost Enabled', 'Priority Boost sounds awesome, but it can actually cause your SQL Server to crash.');
						END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 128 )
                            AND SERVERPROPERTY('EngineEdition') <> 8 
							BEGIN

							IF (@ProductVersionMajor = 14 AND @ProductVersionMinor < 1000) OR
							   (@ProductVersionMajor = 13 AND @ProductVersionMinor < 4001) OR
							   (@ProductVersionMajor = 12 AND @ProductVersionMinor < 5000) OR
							   (@ProductVersionMajor = 11 AND @ProductVersionMinor < 6020) OR
							   (@ProductVersionMajor = 10.5 AND @ProductVersionMinor < 6000) OR
							   (@ProductVersionMajor = 10 AND @ProductVersionMinor < 6000) OR
							   (@ProductVersionMajor = 9 )
								BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 128) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults(CheckID, Priority, FindingsGroup, Finding, Details)
									VALUES(128, 20, 'Reliability', 'Unsupported Build of SQL Server', 'Version ' + CAST(@ProductVersionMajor AS VARCHAR(100)) + '.' +
										CASE WHEN @ProductVersionMajor > 9 THEN
										CAST(@ProductVersionMinor AS VARCHAR(100)) + ' is no longer supported by Microsoft. You need to apply a service pack.'
										ELSE ' is no longer support by Microsoft. 
										You should be making plans to upgrade to a modern version of SQL Server.' END);
								END;

							END;
							
						
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 129 )
                            AND SERVERPROPERTY('EngineEdition') <> 8 
							BEGIN
							IF (@ProductVersionMajor = 11 AND @ProductVersionMinor >= 3000 AND @ProductVersionMinor <= 3436) OR
							   (@ProductVersionMajor = 11 AND @ProductVersionMinor = 5058) OR
							   (@ProductVersionMajor = 12 AND @ProductVersionMinor >= 2000 AND @ProductVersionMinor <= 2342)
								BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 129) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults(CheckID, Priority, FindingsGroup, Finding, Details)
									VALUES(129, 20, 'Reliability', 'Dangerous Build of SQL Server (Corruption)',
									 'There are dangerous known bugs with version ' + CAST(@ProductVersionMajor AS VARCHAR(100)) + '.' + CAST(@ProductVersionMinor AS VARCHAR(100)));
								END;

							END;

						
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 157 )
                            AND SERVERPROPERTY('EngineEdition') <> 8 
							BEGIN
							IF (@ProductVersionMajor = 10 AND @ProductVersionMinor >= 5500 AND @ProductVersionMinor <= 5512) OR
							   (@ProductVersionMajor = 10 AND @ProductVersionMinor >= 5750 AND @ProductVersionMinor <= 5867) OR
							   (@ProductVersionMajor = 10.5 AND @ProductVersionMinor >= 4000 AND @ProductVersionMinor <= 4017) OR
							   (@ProductVersionMajor = 10.5 AND @ProductVersionMinor >= 4251 AND @ProductVersionMinor <= 4319) OR
							   (@ProductVersionMajor = 11 AND @ProductVersionMinor >= 3000 AND @ProductVersionMinor <= 3129) OR
							   (@ProductVersionMajor = 11 AND @ProductVersionMinor >= 3300 AND @ProductVersionMinor <= 3447) OR
							   (@ProductVersionMajor = 12 AND @ProductVersionMinor >= 2000 AND @ProductVersionMinor <= 2253) OR
							   (@ProductVersionMajor = 12 AND @ProductVersionMinor >= 2300 AND @ProductVersionMinor <= 2370)
								BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 157) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults(CheckID, Priority, FindingsGroup, Finding, Details)
									VALUES(157, 20, 'Reliability', 'Dangerous Build of SQL Server (Security)', 
									'There are dangerous known bugs with version ' + CAST(@ProductVersionMajor AS VARCHAR(100)) + '.' + CAST(@ProductVersionMinor AS VARCHAR(100)) );
								END;

							END;
						
						
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 189 )
                            AND SERVERPROPERTY('EngineEdition') <> 8
							BEGIN
							IF (@ProductVersionMajor = 13 AND @ProductVersionMinor < 4001 AND @@VERSION LIKE '%Standard Edition%')
								BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 189) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults(CheckID, Priority, FindingsGroup, Finding, Details)
									VALUES(189, 100, 'Features', 'Missing Features', 'SQL 2016 Standard Edition is being used but not Service Pack 1');
								END;

							END;						
                        
                        
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 216 )
                            AND SERVERPROPERTY('EngineEdition') <> 8
							BEGIN
							IF (@ProductVersionMajor = 14 AND @ProductVersionMinor < 3015)
								BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 216) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults(CheckID, Priority, FindingsGroup, Finding, Details)
									VALUES(216, 100, 'Features', 'Missing Features', 'SQL 2017 is being used but not Cumulative Update 3. ');
								END;

							END;		

                        
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 217 )
                            AND SERVERPROPERTY('EngineEdition') NOT IN (5,8) 
                            AND EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'SqlServerVersions' AND TABLE_TYPE = 'BASE TABLE')
                            AND NOT EXISTS (SELECT * FROM #SQLCheckResults WHERE CheckID IN (128, 129, 157, 189, 216)) 
							BEGIN
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 217) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults(CheckID, Priority, FindingsGroup, Finding, Details)
                                    SELECT TOP 1 217, 100, 'Reliability', 'Cumulative Update Available',v.MinorVersionName + ' was released on ' + CAST(CONVERT(DATETIME, v.ReleaseDate, 112) AS VARCHAR(100))
                                    FROM dbo.SqlServerVersions v
                                    WHERE v.MajorVersionNumber = @ProductVersionMajor
                                      AND v.MinorVersionNumber > @ProductVersionMinor
                                    ORDER BY v.MinorVersionNumber DESC;
							END;		

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 145 )
	                        AND EXISTS ( SELECT *
					                        FROM   sys.all_objects o
					                        WHERE  o.name = 'dm_db_xtp_table_memory_stats' )
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 145) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			                        SELECT 145 AS CheckID,
			                        10 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''High Memory Use for In-Memory OLTP (Hekaton)'' AS Finding,
			                        CAST(CAST((SUM(mem.pages_kb / 1024.0) / CAST(value_in_use AS INT) * 100) AS INT) AS NVARCHAR(100)) + ''% of your '' + CAST(CAST((CAST(value_in_use AS DECIMAL(38,1)) / 1024) AS MONEY) AS NVARCHAR(100)) + ''GB of your max server memory is being used for in-memory OLTP tables (Hekaton). Microsoft recommends having 2X your Hekaton table space available in memory just for Hekaton, with a max of 250GB of in-memory data regardless of your server memory capacity.'' AS Details
			                        FROM sys.configurations c INNER JOIN sys.dm_os_memory_clerks mem ON mem.type = ''MEMORYCLERK_XTP''
                                    WHERE c.name = ''max server memory (MB)''
                                    GROUP BY c.value_in_use
                                    HAVING CAST(value_in_use AS DECIMAL(38,2)) * .25 < SUM(mem.pages_kb / 1024.0)
                                      OR SUM(mem.pages_kb / 1024.0) > 250000 OPTION (RECOMPILE)';
		
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 146 )
	                        AND EXISTS ( SELECT *
					                        FROM   sys.all_objects o
					                        WHERE  o.name = 'dm_db_xtp_table_memory_stats' )
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 146) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			                        SELECT 146 AS CheckID,
			                        200 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''In-Memory OLTP (Hekaton) In Use'' AS Finding,
			                        CAST(CAST((SUM(mem.pages_kb / 1024.0) / CAST(value_in_use AS INT) * 100) AS INT) AS NVARCHAR(100)) + ''% of your '' + CAST(CAST((CAST(value_in_use AS DECIMAL(38,1)) / 1024) AS MONEY) AS NVARCHAR(100)) + ''GB of your max server memory is being used for in-memory OLTP tables (Hekaton).'' AS Details
			                        FROM sys.configurations c INNER JOIN sys.dm_os_memory_clerks mem ON mem.type = ''MEMORYCLERK_XTP''
                                    WHERE c.name = ''max server memory (MB)''
                                    GROUP BY c.value_in_use
                                    HAVING SUM(mem.pages_kb / 1024.0) > 10 OPTION (RECOMPILE)';
		
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 147 )
	                        AND EXISTS ( SELECT *
					                        FROM   sys.all_objects o
					                        WHERE  o.name = 'dm_xtp_transaction_stats' )
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 147) WITH NOWAIT
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			                        SELECT 147 AS CheckID,
			                        100 AS Priority,
			                        ''In-Memory OLTP (Hekaton)'' AS FindingsGroup,
			                        ''Transaction Errors'' AS Finding,
			                        ''Since restart: '' + CAST(validation_failures AS NVARCHAR(100)) + '' validation failures, '' + CAST(dependencies_failed AS NVARCHAR(100)) + '' dependency failures, '' + CAST(write_conflicts AS NVARCHAR(100)) + '' write conflicts, '' + CAST(unique_constraint_violations AS NVARCHAR(100)) + '' unique constraint violations.'' AS Details
			                        FROM sys.dm_xtp_transaction_stats
                                    WHERE validation_failures <> 0
                                            OR dependencies_failed <> 0
                                            OR write_conflicts <> 0
                                            OR unique_constraint_violations <> 0 OPTION (RECOMPILE);';
		
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 148 )
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 148) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
				                        ( CheckID ,
					                        DatabaseName ,
					                        Priority ,
					                        FindingsGroup ,
					                        Finding ,
					                        Details
				                        )
				                        SELECT DISTINCT 148 AS CheckID ,
						                        d.[name] AS DatabaseName ,
						                        170 AS Priority ,
						                        'Reliability' AS FindingsGroup ,
						                        'Database Files on Network File Shares' AS Finding ,
						                        ( 'Files for this database are on: ' + LEFT(mf.physical_name, 30)) AS Details
				                        FROM    sys.databases d
                                          INNER JOIN sys.master_files mf ON d.database_id = mf.database_id
				                        WHERE mf.physical_name LIKE '\\%'
						                        AND d.name NOT IN ( SELECT DISTINCT
													                        DatabaseName
											                        FROM    #SkipChecks
																	WHERE CheckID IS NULL OR CheckID = 148);
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 149 )
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 149) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
				                        ( CheckID ,
					                        DatabaseName ,
					                        Priority ,
					                        FindingsGroup ,
					                        Finding ,
					                        Details
				                        )
				                        SELECT DISTINCT 149 AS CheckID ,
						                        d.[name] AS DatabaseName ,
						                        170 AS Priority ,
						                        'Reliability' AS FindingsGroup ,
						                        'Database Files Stored in Azure' AS Finding ,
						                        ( 'Files for this database are on: ' + LEFT(mf.physical_name, 30)) AS Details
				                        FROM    sys.databases d
                                          INNER JOIN sys.master_files mf ON d.database_id = mf.database_id
				                        WHERE mf.physical_name LIKE 'http://%'
						                        AND d.name NOT IN ( SELECT DISTINCT
													                        DatabaseName
											                        FROM    #SkipChecks
																	WHERE CheckID IS NULL OR CheckID = 149);
	                        END;

                        

						
						BEGIN TRY
						
						INSERT INTO #fnTraceGettable
							(	TextData ,
								DatabaseName ,
								EventClass ,
								Severity ,
								StartTime ,
								EndTime ,
								Duration ,
								NTUserName ,
								NTDomainName ,
								HostName ,
								ApplicationName ,
								LoginName ,
								DBUserName
							)
							SELECT TOP 20000
								CONVERT(NVARCHAR(4000),t.TextData) ,
								t.DatabaseName ,
								t.EventClass ,
								t.Severity ,
								t.StartTime ,
								t.EndTime ,
								t.Duration ,
								t.NTUserName ,
								t.NTDomainName ,
								t.HostName ,
								t.ApplicationName ,
								t.LoginName ,
								t.DBUserName
							FROM sys.fn_trace_gettable(@base_tracefilename, DEFAULT) t
							WHERE
							(
								t.EventClass = 22
								AND t.Severity >= 17
								AND t.StartTime > DATEADD(dd, -30, GETDATE())
							)
							OR
							(
							    t.EventClass IN (92, 93)
                                AND t.StartTime > DATEADD(dd, -30, GETDATE())
                                AND t.Duration > 15000000
							)
							OR
							(
								t.EventClass IN (94, 95, 116)
							)

							SET @TraceFileIssue = 0

						END TRY
						BEGIN CATCH

							SET @TraceFileIssue = 1
						
						END CATCH
											
						IF @TraceFileIssue = 1
							BEGIN
						IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 199 )								
								
								INSERT  INTO #SQLCheckResults
								            ( CheckID ,
								                DatabaseName ,
								                Priority ,
								                FindingsGroup ,
								                Finding ,
								                Details
								            )
											SELECT
												'199' AS CheckID ,
												'' AS DatabaseName ,
												50 AS Priority ,
												'Reliability' AS FindingsGroup ,
												'There Is An Error With The Default Trace' AS Finding ,
												'Check the files are present at ' + @base_tracefilename AS Details
							END
						
						IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 150 )
                            AND @base_tracefilename IS NOT NULL
							AND @TraceFileIssue = 0
	                        BEGIN

		                        IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 150) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
				                        ( CheckID ,
					                        DatabaseName ,
					                        Priority ,
					                        FindingsGroup ,
					                        Finding ,
					                        Details
				                        )
				                        SELECT DISTINCT 150 AS CheckID ,
					                            t.DatabaseName,
						                        50 AS Priority ,
						                        'Reliability' AS FindingsGroup ,
						                        'Errors Logged Recently in the Default Trace' AS Finding ,
						                        CAST(t.TextData AS NVARCHAR(4000)) AS Details
                                        FROM    #fnTraceGettable t
                                        WHERE t.EventClass = 22
                                          										  --AND t.Severity >= 17
                                          --AND t.StartTime > DATEADD(dd, -30, GETDATE());
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 151 )
                            AND @base_tracefilename IS NOT NULL
							AND @TraceFileIssue = 0
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 151) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
				                        ( CheckID ,
					                        DatabaseName ,
					                        Priority ,
					                        FindingsGroup ,
					                        Finding ,
					                        Details
				                        )
				                        SELECT DISTINCT 151 AS CheckID ,
					                            t.DatabaseName,
						                        50 AS Priority ,
						                        'Performance' AS FindingsGroup ,
						                        'File Growths Slow' AS Finding ,
						                        CAST(COUNT(*) AS NVARCHAR(100)) + ' growths took more than 15 seconds each. Consider setting file autogrowth to a smaller increment.' AS Details
                                        FROM    #fnTraceGettable t
                                        WHERE t.EventClass IN (92, 93)
                                         
										  --AND t.StartTime > DATEADD(dd, -30, GETDATE())
                                          --AND t.Duration > 15000000
                                        GROUP BY t.DatabaseName
                                        HAVING COUNT(*) > 1;
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 160 )
                            AND EXISTS (SELECT * FROM sys.all_columns WHERE name = 'query_hash')
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 160) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			                        SELECT TOP 1 160 AS CheckID,
			                        100 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''Many Plans for One Query'' AS Finding,
			                        CAST(COUNT(DISTINCT plan_handle) AS NVARCHAR(50)) + '' plans are present for a single query in the plan cache - meaning we probably have parameterization issues.'' AS Details
			                        FROM sys.dm_exec_query_stats qs
                                    CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) pa
                                    WHERE pa.attribute = ''dbid''
                                    GROUP BY qs.query_hash, pa.value
                                    HAVING COUNT(DISTINCT plan_handle) > 50
									ORDER BY COUNT(DISTINCT plan_handle) DESC OPTION (RECOMPILE);';
		
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
	                        END;

                        
                        IF NOT EXISTS ( SELECT  1
				                        FROM    #SkipChecks
				                        WHERE   DatabaseName IS NULL AND CheckID = 161 )
	                        BEGIN
		
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 161) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			                        SELECT TOP 1 161 AS CheckID,
			                        100 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''High Number of Cached Plans'' AS Finding,
			                        ''Your server configuration is limited to '' + CAST(ht.buckets_count * 4 AS VARCHAR(20)) + '' '' + ht.name + '', and you are currently caching '' + CAST(cc.entries_count AS VARCHAR(20)) + ''.'' AS Details
			                        FROM sys.dm_os_memory_cache_hash_tables ht
			                        INNER JOIN sys.dm_os_memory_cache_counters cc ON ht.name = cc.name AND ht.type = cc.type
			                        where ht.name IN ( ''SQL Plans'' , ''Object Plans'' , ''Bound Trees'' )
			                        AND cc.entries_count >= (3 * ht.buckets_count) OPTION (RECOMPILE)';
		
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
	                        END;

						
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 165 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 165) WITH NOWAIT;
								
								INSERT INTO #SQLCheckResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									Details)
								SELECT 165, 50, 'Performance', 'Too Much Free Memory',CAST((CAST(cFree.cntr_value AS BIGINT) / 1024 / 1024 ) AS NVARCHAR(100)) + N'GB of free memory inside SQL Server''s buffer pool, which is ' + CAST((CAST(cTotal.cntr_value AS BIGINT) / 1024 / 1024) AS NVARCHAR(100)) + N'GB. You would think lots of free memory would be good, but its not always good' AS Details
								FROM sys.dm_os_performance_counters cFree
								INNER JOIN sys.dm_os_performance_counters cTotal ON cTotal.object_name LIKE N'%Memory Manager%'
									AND cTotal.counter_name = N'Total Server Memory (KB)                                                                                                        '
								WHERE cFree.object_name LIKE N'%Memory Manager%'
									AND cFree.counter_name = N'Free Memory (KB)                                                                                                                '
									AND CAST(cTotal.cntr_value AS BIGINT) > 20480000000
									AND CAST(cTotal.cntr_value AS BIGINT) * .3 <= CAST(cFree.cntr_value AS BIGINT)
                                    AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Standard%';

							END;

                        
						IF @Debug IN (1, 2) RAISERROR('Generating database defaults.', 0, 1) WITH NOWAIT;
						
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_supplemental_logging_enabled', 0, 131, 210, 'Supplemental Logging Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_supplemental_logging_enabled' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'snapshot_isolation_state', 0, 132, 210, 'Snapshot Isolation Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'snapshot_isolation_state' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_read_committed_snapshot_on', 0, 133, 210, 'Read Committed Snapshot Isolation Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_read_committed_snapshot_on' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_auto_create_stats_incremental_on', 0, 134, 210, 'Auto Create Stats Incremental Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_auto_create_stats_incremental_on' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_ansi_null_default_on', 0, 135, 210, 'ANSI NULL Default Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_ansi_null_default_on' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_recursive_triggers_on', 0, 136, 210, 'Recursive Triggers Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_recursive_triggers_on' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_trustworthy_on', 0, 137, 210, 'Trustworthy Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_trustworthy_on' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_parameterization_forced', 0, 138, 210, 'Forced Parameterization Enabled',  NULL
						  FROM sys.all_columns
						  WHERE name = 'is_parameterization_forced' AND object_id = OBJECT_ID('sys.databases');
						
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_cdc_enabled', 0, 140, 210, 'Change Data Capture Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_cdc_enabled' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'containment', 0, 141, 210, 'Containment Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'containment' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'target_recovery_time_in_seconds', 0, 142, 210, 'Target Recovery Time Changed', NULL
						  FROM sys.all_columns
						  WHERE name = 'target_recovery_time_in_seconds' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'delayed_durability', 0, 143, 210, 'Delayed Durability Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'delayed_durability' AND object_id = OBJECT_ID('sys.databases');
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_memory_optimized_elevate_to_snapshot_on', 0, 144, 210, 'Memory Optimized Enabled', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_memory_optimized_elevate_to_snapshot_on' AND object_id = OBJECT_ID('sys.databases');

						DECLARE DatabaseDefaultsLoop CURSOR FOR
						  SELECT name, DefaultValue, CheckID, Priority, Finding, Details
						  FROM #DatabaseDefaults;

						OPEN DatabaseDefaultsLoop;
						FETCH NEXT FROM DatabaseDefaultsLoop into @CurrentName, @CurrentDefaultValue, @CurrentCheckID, @CurrentPriority, @CurrentFinding, @CurrentDetails;
						WHILE @@FETCH_STATUS = 0
						BEGIN

							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, @CurrentCheckID) WITH NOWAIT;

							
						    IF @CurrentCheckID = 142
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, Details)
								   SELECT ' + CAST(@CurrentCheckID AS NVARCHAR(200)) + ', d.[name], ' + CAST(@CurrentPriority AS
								    NVARCHAR(200)) + ', ''Non-Default Database Config'', ''' + @CurrentFinding + ''',''' + 
								    COALESCE(@CurrentDetails, 'This database setting is not the default.') + '''
									FROM sys.databases d
									WHERE d.database_id > 4 AND (d.[' + @CurrentName + '] 
									NOT IN (0, 60) OR d.[' + @CurrentName + '] IS NULL) OPTION (RECOMPILE);';
							ELSE
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, Details)
								   SELECT ' + CAST(@CurrentCheckID AS NVARCHAR(200)) + ', d.[name], ' + CAST(@CurrentPriority AS NVARCHAR(200)) 
								   + ', ''Non-Default Database Config'', ''' + @CurrentFinding + ''',''' + COALESCE(@CurrentDetails, 'This database setting is not the default.') + '''
									FROM sys.databases d
									WHERE d.database_id > 4 AND (d.[' + @CurrentName + '] <> ' + @CurrentDefaultValue + ' OR d.[' + @CurrentName + '] IS NULL) OPTION (RECOMPILE);';
						
							IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
							IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
							
							EXEC (@StringToExecute);

						FETCH NEXT FROM DatabaseDefaultsLoop into @CurrentName, @CurrentDefaultValue, @CurrentCheckID, @CurrentPriority, 
						@CurrentFinding, @CurrentDetails;
						END;

						CLOSE DatabaseDefaultsLoop;
						DEALLOCATE DatabaseDefaultsLoop;
							


IF @ProductVersionMajor >= 10
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 167 )
					BEGIN
					IF EXISTS ( SELECT  1
											FROM    sys.all_objects
											WHERE   name = 'dm_server_services' )
									BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 167) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							167 AS [CheckID] ,
							250 AS [Priority] ,
							'Server Info' AS [FindingsGroup] ,
							'Agent is Currently Offline' AS [Finding] ,
							( 'Oops! It looks like the ' + [servicename] + ' service is ' + [status_desc] + '. The startup type is ' + [startup_type_desc] + '.'
							   ) AS [Details]
						  FROM
							[sys].[dm_server_services]
						  WHERE [status_desc] <> 'Running'
						  AND [servicename] LIKE 'SQL Server Agent%'
						  AND CAST(SERVERPROPERTY('Edition') AS VARCHAR(1000)) NOT LIKE '%xpress%';

					END;
				END;

IF @ProductVersionMajor >= 10
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 168 )
					BEGIN
					IF EXISTS ( SELECT  1
											FROM    sys.all_objects
											WHERE   name = 'dm_server_services' )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 168) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							168 AS [CheckID] ,
							250 AS [Priority] ,
							'Server Info' AS [FindingsGroup] ,
							'Full-text Filter Daemon Launcher is Currently Offline' AS [Finding] ,
							( 'Oops! It looks like the ' + [servicename] + ' service is ' + [status_desc] + '. The startup type is ' + [startup_type_desc] + '.'
							   ) AS [Details]
						  FROM
							[sys].[dm_server_services]
						  WHERE [status_desc] <> 'Running'
						  AND [servicename] LIKE 'SQL Full-text Filter Daemon Launcher%';

					END;
					END;


IF @ProductVersionMajor >= 10
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 169 )

					BEGIN
					IF EXISTS ( SELECT  1
											FROM    sys.all_objects
											WHERE   name = 'dm_server_services' )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 169) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							169 AS [CheckID] ,
							250 AS [Priority] ,
							'Informational' AS [FindingsGroup] ,
							'SQL Server is running under an NT Service account' AS [Finding] ,
							( 'I''m running as ' + [service_account] + '. I wish I had an Active Directory service account instead.'
							   ) AS [Details]
						  FROM
							[sys].[dm_server_services]
						  WHERE [service_account] LIKE 'NT Service%'
						  AND [servicename] LIKE 'SQL Server%'
						  AND [servicename] NOT LIKE 'SQL Server Agent%';

					END;
					END;


IF @ProductVersionMajor >= 10
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 170 )

					BEGIN
					IF EXISTS ( SELECT  1
											FROM    sys.all_objects
											WHERE   name = 'dm_server_services' )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 170) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							170 AS [CheckID] ,
							250 AS [Priority] ,
							'Informational' AS [FindingsGroup] ,
							'SQL Server Agent is running under an NT Service account' AS [Finding] ,
							( 'I''m running as ' + [service_account] + '. I wish I had an Active Directory service account instead.'
							   ) AS [Details]
						  FROM
							[sys].[dm_server_services]
						  WHERE [service_account] LIKE 'NT Service%'
						  AND [servicename] LIKE 'SQL Server Agent%';

					END;
					END;


IF @ProductVersionMajor >= 10
               AND NOT (@ProductVersionMajor = 10.5 AND @ProductVersionMinor < 4297) 
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 171 )
					BEGIN
					IF EXISTS ( SELECT  1
											FROM    sys.all_objects
											WHERE   name = 'dm_server_memory_dumps' )
						BEGIN
							IF 5 <= (SELECT COUNT(*) FROM [sys].[dm_server_memory_dumps] WHERE [creation_time] >= DATEADD(YEAR, -1, GETDATE()))

							BEGIN
							
							  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 171) WITH NOWAIT;
							
							  INSERT    INTO [#SQLCheckResults]
										( [CheckID] ,
										  [Priority] ,
										  [FindingsGroup] ,
										  [Finding] ,
										  [Details] )

								SELECT
								171 AS [CheckID] ,
								20 AS [Priority] ,
								'Reliability' AS [FindingsGroup] ,
								'Memory Dumps Have Occurred' AS [Finding] ,
								( 'That ain''t good. I''ve had ' +
									CAST(COUNT(*) AS VARCHAR(100)) + ' memory dumps between ' +
									CAST(CAST(MIN([creation_time]) AS DATETIME) AS VARCHAR(100)) +
									' and ' +
									CAST(CAST(MAX([creation_time]) AS DATETIME) AS VARCHAR(100)) +
									'!'
								   ) AS [Details]
							  FROM
								[sys].[dm_server_memory_dumps]
							  WHERE [creation_time] >= DATEADD(year, -1, GETDATE());

							END;
						END;
					END;


					IF	NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 173 )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 173) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							173 AS [CheckID] ,
							200 AS [Priority] ,
							'Licensing' AS [FindingsGroup] ,
							'Non-Production License' AS [Finding] ,
							( 'We''re not the licensing police, but if this is supposed to be a production server, and you''re running ' +
							CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) +
							' the good folks at Microsoft might get upset with you. Better start counting those cores.'
							   ) AS [Details]
							WHERE CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) LIKE '%Developer%'
							OR CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) LIKE '%Evaluation%';

					END;


			IF @ProductVersionMajor >= 12
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 174 )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 174) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							174 AS [CheckID] ,
							200 AS [Priority] ,
							'Performance' AS [FindingsGroup] ,
							'Buffer Pool Extensions Enabled' AS [Finding] ,
							( 'You have Buffer Pool Extensions enabled, and one lives here: ' +
								[path] +
								'. It''s currently ' +
								CASE WHEN [current_size_in_kb] / 1024. / 1024. > 0
																	 THEN CAST([current_size_in_kb] / 1024. / 1024. AS VARCHAR(100))
																		  + ' GB'
																	 ELSE CAST([current_size_in_kb] / 1024. AS VARCHAR(100))
																		  + ' MB'
								END +
								'. Did you know that BPEs only provide single threaded access 8KB (one page) at a time?'	
							   ) AS [Details]
							 FROM sys.dm_os_buffer_pool_extension_configuration
							 WHERE [state_description] <> 'BUFFER POOL EXTENSION DISABLED';

					END;


			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 175 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 175) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
										SELECT DISTINCT
										175 AS CheckID ,
										'TempDB' AS DatabaseName ,
										170 AS Priority ,
										'File Configuration' AS FindingsGroup ,
										'TempDB Has >16 Data Files' AS Finding ,
										'TempDB has ' + CAST(COUNT_BIG(*) AS VARCHAR(30)) + '. Did you forget to terminate a loop somewhere?' AS Details
								  FROM sys.[master_files] AS [mf]
								  WHERE [mf].[database_id] = 2 AND [mf].[type] = 0
								  HAVING COUNT_BIG(*) > 16;
					END;	

			IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 176 )
								BEGIN
			
			IF EXISTS ( SELECT  1
														FROM    sys.all_objects
														WHERE   name = 'dm_xe_sessions' )
								
								BEGIN
									
									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 176) WITH NOWAIT;
									
									INSERT  INTO #SQLCheckResults
											( CheckID ,
											  DatabaseName ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  Details
											)
													SELECT DISTINCT
													176 AS CheckID ,
													'' AS DatabaseName ,
													200 AS Priority ,
													'Monitoring' AS FindingsGroup ,
													'Extended Events Hyperextension' AS Finding ,
													'You have ' + CAST(COUNT_BIG(*) AS VARCHAR(30)) + ' Extended Events sessions running. You sure you meant to do that?' AS Details
											    FROM sys.dm_xe_sessions
												WHERE [name] NOT IN
												( 'AlwaysOn_health', 
												  'system_health', 
												  'telemetry_xevents', 
												  'sp_server_diagnostics', 
												  'sp_server_diagnostics session', 
												  'hkenginexesession' )
												AND name NOT LIKE '%$A%'
											  HAVING COUNT_BIG(*) >= 2;
								END;	
								END;
			
			
			IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 177 )
								BEGIN
								
								IF EXISTS ( SELECT  1
														FROM    sys.all_objects
														WHERE   name = 'dm_server_registry' )
			
								BEGIN
									
									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 177) WITH NOWAIT;
									
									INSERT  INTO #SQLCheckResults
											( CheckID ,
											  DatabaseName ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  Details
											)
													SELECT DISTINCT
													177 AS CheckID ,
													'' AS DatabaseName ,
													5 AS Priority ,
													'Monitoring' AS FindingsGroup ,
													'Disabled Internal Monitoring Features' AS Finding ,
													'You have -x as a startup parameter. ' AS Details
													FROM
													[sys].[dm_server_registry] AS [dsr]
													WHERE
													[dsr].[registry_key] LIKE N'%MSSQLServer\Parameters'
													AND [dsr].[value_data] = '-x';;
								END;		
								END;
			
			
			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 179 )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 179) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							179 AS [CheckID] ,
							5 AS [Priority] ,
							'Reliability' AS [FindingsGroup] ,
							'Dangerous Third Party Modules' AS [Finding] ,
							( COALESCE(company, '') + ' - ' + COALESCE(description, '') + ' - ' + COALESCE(name, '') + ' - suspected dangerous third party module is installed.') AS [Details]
							FROM sys.dm_os_loaded_modules
							WHERE UPPER(name) LIKE UPPER('%\ENTAPI.DLL') 
							OR UPPER(name) LIKE UPPER('%\HIPI.DLL') OR UPPER(name) LIKE UPPER('%\HcSQL.dll') OR UPPER(name) LIKE UPPER('%\HcApi.dll') OR UPPER(name) LIKE UPPER('%\HcThe.dll') 
							OR UPPER(name) LIKE UPPER('%\SOPHOS_DETOURED.DLL') OR UPPER(name) LIKE UPPER('%\SOPHOS_DETOURED_x64.DLL') OR UPPER(name) LIKE UPPER('%\SWI_IFSLSP_64.dll') OR UPPER(name) LIKE UPPER('%\SOPHOS~%.dll') 
							OR UPPER(name) LIKE UPPER('%\PIOLEDB.DLL') OR UPPER(name) LIKE UPPER('%\PISDK.DLL'); 

					END;

			

			IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 180 )
							AND CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) LIKE '1%' 
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 180) WITH NOWAIT;
					
						WITH XMLNAMESPACES ('www.microsoft.com/SqlServer/Dts' AS [dts])
						,[maintenance_plan_steps] AS (
							SELECT [name]
								, [id] 
								, CAST(CAST([packagedata] AS VARBINARY(MAX)) AS XML) AS [maintenance_plan_xml]
							FROM [msdb].[dbo].[sysssispackages]
							WHERE [packagetype] = 6
						   )
							INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
										[Priority] ,
										[FindingsGroup] ,
										[Finding] ,
										[Details] )									
						SELECT
						180 AS [CheckID] ,
						
						
						CASE WHEN (cast(datediff(dd, substring(cast(sjh.run_date as nvarchar(10)), 1, 4) + '-' + substring(cast(sjh.run_date as nvarchar(10)), 5, 2) + '-' + substring(cast(sjh.run_date as nvarchar(10)), 7, 2), GETDATE()) AS INT) < 30) OR (j.[enabled] = 1 AND ssc.[enabled] = 1 )THEN
						    100
						ELSE 
					        200
						END AS Priority,
						'Performance' AS [FindingsGroup] ,
						'Shrink Database Step In Maintenance Plan' AS [Finding] ,
						'The maintenance plan ' + [mps].[name] + ' has a step to shrink databases in it. Shrinking databases is as outdated as maintenance plans.'
						+ CASE WHEN COALESCE(ssc.name,'0') != '0' THEN + ' (Schedule: [' + ssc.name + '])' ELSE + '' END AS [Details]
						FROM [maintenance_plan_steps] [mps]
							CROSS APPLY [maintenance_plan_xml].[nodes]('//dts:Executables/dts:Executable') [t]([c])
                    	join msdb.dbo.sysmaintplan_subplans as sms
                    		on mps.id = sms.plan_id
                    	JOIN msdb.dbo.sysjobs j
                    		on sms.job_id = j.job_id
                    	LEFT OUTER JOIN msdb.dbo.sysjobsteps AS step
                    		ON j.job_id = step.job_id
                    	LEFT OUTER JOIN msdb.dbo.sysjobschedules AS sjsc
                    		ON j.job_id = sjsc.job_id
                    	LEFT OUTER JOIN msdb.dbo.sysschedules AS ssc
                    		ON sjsc.schedule_id = ssc.schedule_id
                    		AND sjsc.job_id = j.job_id
                    	LEFT OUTER JOIN msdb.dbo.sysjobhistory AS sjh
                    		ON j.job_id = sjh.job_id
                    		AND step.step_id = sjh.step_id
                    		AND sjh.run_date IN (SELECT max(sjh2.run_date) FROM msdb.dbo.sysjobhistory AS sjh2 WHERE sjh2.job_id = j.job_id) -- get the latest entry date
                    		AND sjh.run_time IN (SELECT max(sjh3.run_time) FROM msdb.dbo.sysjobhistory AS sjh3 WHERE sjh3.job_id = j.job_id AND sjh3.run_date = sjh.run_date) -- get the latest entry time
						WHERE [c].[value]('(@dts:ObjectName)', 'VARCHAR(128)') = 'Shrink Database Task';

						END;

		
		IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 181 )
						AND CONVERT(VARCHAR(128), SERVERPROPERTY ('productversion')) LIKE '1%' 
				BEGIN
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 181) WITH NOWAIT;

						WITH XMLNAMESPACES ('www.microsoft.com/SqlServer/Dts' AS [dts])
						,[maintenance_plan_steps] AS (
							SELECT [name]
								, CAST(CAST([packagedata] AS VARBINARY(MAX)) AS XML) AS [maintenance_plan_xml]
							FROM [msdb].[dbo].[sysssispackages]
							WHERE [packagetype] = 6
							), [maintenance_plan_table] AS (
						SELECT [mps].[name]
							,[c].[value]('(@dts:ObjectName)', 'NVARCHAR(128)') AS [step_name]
						FROM [maintenance_plan_steps] [mps]
							CROSS APPLY [maintenance_plan_xml].[nodes]('//dts:Executables/dts:Executable') [t]([c])
						), [mp_steps_pretty] AS (SELECT DISTINCT [m1].[name] ,
								STUFF((SELECT N', ' + [m2].[step_name]  FROM [maintenance_plan_table] AS [m2] WHERE [m1].[name] = [m2].[name]
								FOR XML PATH(N'')), 1, 2, N'') AS [maintenance_plan_steps]
						FROM [maintenance_plan_table] AS [m1])
						
							INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
										[Priority] ,
										[FindingsGroup] ,
										[Finding] ,
										[Details] )						
						
						SELECT
						181 AS [CheckID] ,
						100 AS [Priority] ,
						'Performance' AS [FindingsGroup] ,
						'Repetitive Steps In Maintenance Plans' AS [Finding] ,
						'The maintenance plan ' + [m].[name] + ' is doing repetitive work on indexes and statistics. Perhaps it''s time to try something more modern?' AS [Details]
						FROM [mp_steps_pretty] m
						WHERE m.[maintenance_plan_steps] LIKE '%Rebuild%Reorganize%'
						OR m.[maintenance_plan_steps] LIKE '%Rebuild%Update%';

						END;
			

			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 184 )
				AND CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) NOT LIKE '10%'
				AND CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) NOT LIKE '9%'
					BEGIN
		                        IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 184) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			                        							SELECT TOP 1
							  184 AS CheckID ,
							  20 AS Priority ,
							  ''Reliability'' AS FindingsGroup ,
							  ''No Failover Cluster Nodes Available'' AS Finding ,
							   ''There are no failover cluster nodes available if the active node fails'' AS Details
							FROM (
							  SELECT SUM(CASE WHEN [status] = 0 AND [is_current_owner] = 0 THEN 1 ELSE 0 END) AS [available_nodes]
							  FROM sys.dm_os_cluster_nodes
							) a
							WHERE [available_nodes] < 1 OPTION (RECOMPILE)';
		
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
					END;


		IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 191 )
			AND (SELECT COUNT(*) FROM sys.master_files WHERE database_id = 2) <> (SELECT COUNT(*) FROM tempdb.sys.database_files)
				BEGIN
					
					IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 191) WITH NOWAIT
					
					INSERT    INTO [#SQLCheckResults]
							( [CheckID] ,
								[Priority] ,
								[FindingsGroup] ,
								[Finding] ,
								[Details] )						
						
						SELECT
						191 AS [CheckID] ,
						50 AS [Priority] ,
						'Reliability' AS [FindingsGroup] ,
						'TempDB File Error' AS [Finding] ,
						'Mismatch between the number of TempDB files in sys.master_files versus tempdb.sys.database_files' AS [Details];
				END;


		IF NOT EXISTS ( SELECT  1
		                FROM    #SkipChecks
		                WHERE   DatabaseName IS NULL
		                        AND CheckID = 198 )
		   AND EXISTS ( SELECT  1
		                FROM    sys.dm_os_schedulers
		                WHERE   is_online = 1
		                        AND scheduler_id < 255
		                        AND parent_node_id < 64
		                GROUP BY parent_node_id,
		                        is_online
		                HAVING  ( COUNT(cpu_id) + 2 ) % 2 = 1 )
		   BEGIN
		
		         IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 198) WITH NOWAIT
				
				 INSERT INTO #SQLCheckResults
		                (
		                  CheckID,
		                  DatabaseName,
		                  Priority,
		                  FindingsGroup,
		                  Finding,
		                  Details
				        )
		         SELECT 198 AS CheckID,
		                NULL AS DatabaseName,
		                10 AS Priority,
		                'Performance' AS FindingsGroup,
		                'CPU w/Odd Number of Cores' AS Finding,
		                'Node ' + CONVERT(VARCHAR(10), parent_node_id) + ' has ' + CONVERT(VARCHAR(10), COUNT(cpu_id))
		                + CASE WHEN COUNT(cpu_id) = 1 THEN ' core assigned to it. This is a really bad NUMA configuration.'
		                       ELSE ' cores assigned to it. This is a really bad NUMA configuration.'
		                  END AS Details
		         FROM   sys.dm_os_schedulers
		         WHERE  is_online = 1
		                AND scheduler_id < 255
		                AND parent_node_id < 64
						AND EXISTS (
									SELECT 1
									FROM ( SELECT    memory_node_id, SUM(online_scheduler_count) AS schedulers
									       FROM      sys.dm_os_nodes
									       WHERE     memory_node_id < 64
									       GROUP  BY memory_node_id ) AS nodes
										   HAVING MIN(nodes.schedulers) <> MAX(nodes.schedulers)
									)
		         GROUP BY parent_node_id,
		                is_online
		         HAVING ( COUNT(cpu_id) + 2 ) % 2 = 1;
		
		   END;


		
		
		IF @TraceFileIssue = 0
		BEGIN
		SELECT UPPER(
					REPLACE(
						SUBSTRING(CONVERT(NVARCHAR(MAX), t.TextData), 0,
								ISNULL(
									NULLIF(
										CHARINDEX('(', CONVERT(NVARCHAR(MAX), t.TextData)),
										 0),
									  LEN(CONVERT(NVARCHAR(MAX), t.TextData)) + 1 )) --This replaces everything up to an open paren, if one exists.
										, SUBSTRING(CONVERT(NVARCHAR(MAX), t.TextData),
											ISNULL(
												NULLIF(
													CHARINDEX(' WITH ',CONVERT(NVARCHAR(MAX), t.TextData))
													, 0),
												LEN(CONVERT(NVARCHAR(MAX), t.TextData)) + 1),
													LEN(CONVERT(NVARCHAR(MAX), t.TextData)) + 1 )
					   , '')
					) AS [dbcc_event_trunc_upper],
			UPPER(
				REPLACE(
					CONVERT(NVARCHAR(MAX), t.TextData), SUBSTRING(CONVERT(NVARCHAR(MAX), t.TextData),
											ISNULL(
												NULLIF(
													CHARINDEX(' WITH ',CONVERT(NVARCHAR(MAX), t.TextData))
													, 0),
												LEN(CONVERT(NVARCHAR(MAX), t.TextData)) + 1),
													LEN(CONVERT(NVARCHAR(MAX), t.TextData)) + 1 ), '')) AS [dbcc_event_full_upper],
			MIN(t.StartTime) OVER (PARTITION BY CONVERT(NVARCHAR(128), t.TextData)) AS	min_start_time,
			MAX(t.StartTime) OVER (PARTITION BY CONVERT(NVARCHAR(128), t.TextData)) AS max_start_time,
			t.NTUserName AS [nt_user_name],
			t.NTDomainName AS [nt_domain_name],
			t.HostName AS [host_name],
		    t.ApplicationName AS [application_name],
			t.LoginName [login_name],
			t.DBUserName AS [db_user_name]
		 	INTO #dbcc_events_from_trace
		    FROM #fnTraceGettable AS t
			WHERE t.EventClass = 116
			OPTION(RECOMPILE)
			END;

			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 203 )
							AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 203) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )
			SELECT 203 AS CheckID ,
			        50 AS Priority ,
			        'DBCC Events' AS FindingsGroup ,
			        'Overall Events' AS Finding ,
			        CAST(COUNT(*) AS NVARCHAR(100)) + ' DBCC events have taken place between '
			         + CONVERT(NVARCHAR(30), MIN(d.min_start_time)) + ' and ' + CONVERT(NVARCHAR(30),  MAX(d.max_start_time)) +
					'. This does not include CHECKDB and other usually benign DBCC events.'
					AS Details
			FROM    #dbcc_events_from_trace d
			
			WHERE d.dbcc_event_full_upper NOT LIKE '%DBCC%ADDINSTANCE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%AUTOPILOT%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKALLOC%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKCATALOG%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKCONSTRAINTS%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKDB%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKFILEGROUP%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKIDENT%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKPRIMARYFILE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CHECKTABLE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%CLEANTABLE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%DBINFO%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%ERRORLOG%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%INCREMENTINSTANCE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%INPUTBUFFER%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%LOGINFO%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%OPENTRAN%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%SETINSTANCE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%SHOWFILESTATS%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%SHOW_STATISTICS%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%SQLPERF%NETSTATS%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%SQLPERF%LOGSPACE%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%TRACEON%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%TRACEOFF%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%TRACESTATUS%'
			AND d.dbcc_event_full_upper NOT LIKE '%DBCC%USEROPTIONS%'
			AND d.application_name NOT LIKE 'Critical Care(R) Collector'
			AND d.application_name NOT LIKE '%Red Gate Software Ltd SQL Prompt%'
			AND d.application_name NOT LIKE '%Spotlight Diagnostic Server%'
			AND d.application_name NOT LIKE '%SQL Diagnostic Manager%'
			AND d.application_name NOT LIKE '%Sentry%'
			AND d.application_name NOT LIKE '%LiteSpeed%'
			

			HAVING COUNT(*) > 0;
			
				END;

			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 207 )
							AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 207) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )
					SELECT 207 AS CheckID ,
					        10 AS Priority ,
					        'Performance' AS FindingsGroup ,
					        'DBCC DROPCLEANBUFFERS Ran Recently' AS Finding ,
					        'The user ' + COALESCE(d.nt_user_name, d.login_name) + ' has run DBCC DROPCLEANBUFFERS ' + CAST(COUNT(*) AS NVARCHAR(100)) + ' times between ' + CONVERT(NVARCHAR(30), MIN(d.min_start_time)) + ' and ' + CONVERT(NVARCHAR(30),  MAX(d.max_start_time)) +
							'. If this is a production box, know that you''re clearing all data out of memory when this happens.'
							AS Details
					FROM    #dbcc_events_from_trace d
					WHERE d.dbcc_event_full_upper = N'DBCC DROPCLEANBUFFERS'
					GROUP BY COALESCE(d.nt_user_name, d.login_name)
					HAVING COUNT(*) > 0;

					END;

			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 208 )
							AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 208) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )
					SELECT 208 AS CheckID ,
					        10 AS Priority ,
					        'DBCC Events' AS FindingsGroup ,
					        'DBCC FREEPROCCACHE Ran Recently' AS Finding ,
					        'The user ' + COALESCE(d.nt_user_name, d.login_name) + ' has run DBCC FREEPROCCACHE ' + CAST(COUNT(*) AS NVARCHAR(100)) + ' times between ' + CONVERT(NVARCHAR(30), MIN(d.min_start_time)) + ' and ' + CONVERT(NVARCHAR(30),  MAX(d.max_start_time)) +
							'. This is bad idea'
							AS Details
					FROM    #dbcc_events_from_trace d
					WHERE d.dbcc_event_full_upper = N'DBCC FREEPROCCACHE'
					GROUP BY COALESCE(d.nt_user_name, d.login_name)
					HAVING COUNT(*) > 0;

					END;

			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 205 )
							AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 205) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )
					SELECT 205 AS CheckID ,
					        50 AS Priority ,
					        'Performance' AS FindingsGroup ,
					        'Wait Stats Cleared Recently' AS Finding ,
					        'The user ' + COALESCE(d.nt_user_name, d.login_name) + ' has run DBCC SQLPERF(''SYS.DM_OS_WAIT_STATS'',CLEAR) ' + CAST(COUNT(*) AS NVARCHAR(100)) + ' times between ' + CONVERT(NVARCHAR(30), MIN(d.min_start_time)) + ' and ' + CONVERT(NVARCHAR(30),  MAX(d.max_start_time)) +
							'. Why are you clearing wait stats? '
							AS Details
					FROM    #dbcc_events_from_trace d
					WHERE d.dbcc_event_full_upper = N'DBCC SQLPERF(''SYS.DM_OS_WAIT_STATS'',CLEAR)'
					GROUP BY COALESCE(d.nt_user_name, d.login_name)
					HAVING COUNT(*) > 0;

					END;

			
			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 209 )
							AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 209) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )
						SELECT 209 AS CheckID ,
						        50 AS Priority ,
						        'Reliability' AS FindingsGroup ,
						        'DBCC WRITEPAGE Used Recently' AS Finding ,
						        'The user ' + COALESCE(d.nt_user_name, d.login_name) + ' has run DBCC WRITEPAGE ' + CAST(COUNT(*) AS NVARCHAR(100)) + ' times between ' + CONVERT(NVARCHAR(30), MIN(d.min_start_time)) + ' and ' + CONVERT(NVARCHAR(30),  MAX(d.max_start_time)) +
								' Are they trying to fix corruption, or cause corruption?'
								AS Details
						FROM    #dbcc_events_from_trace d
						WHERE d.dbcc_event_trunc_upper = N'DBCC WRITEPAGE'
						GROUP BY COALESCE(d.nt_user_name, d.login_name)
						HAVING COUNT(*) > 0;

						END;

			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 210 )
							AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 210) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

						SELECT 210 AS CheckID ,
						        10 AS Priority ,
						        'Performance' AS FindingsGroup ,
						        'DBCC SHRINK% Ran Recently' AS Finding ,
						        'The user ' + COALESCE(d.nt_user_name, d.login_name) + ' has run file shrinks ' + CAST(COUNT(*) AS NVARCHAR(100)) + ' times between ' + CONVERT(NVARCHAR(30), MIN(d.min_start_time)) + ' and ' + CONVERT(NVARCHAR(30),  MAX(d.max_start_time)) +
								'Are they trying cause bad performance on purpose?'
								AS Details
						FROM    #dbcc_events_from_trace d
						WHERE d.dbcc_event_trunc_upper LIKE N'DBCC SHRINK%'
						GROUP BY COALESCE(d.nt_user_name, d.login_name)
						HAVING COUNT(*) > 0;

						END;

			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 206 )
						AND @TraceFileIssue = 0
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 206) WITH NOWAIT
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

						SELECT	206 AS CheckID ,
								        10 AS Priority ,
								        'Performance' AS FindingsGroup ,
								        'Auto-Shrink Ran Recently' AS Finding ,
								       	N'The database ' + QUOTENAME(t.DatabaseName) + N' has had '
											+ CONVERT(NVARCHAR(10), COUNT(*))
												+ N' auto shrink events between '
													+ CONVERT(NVARCHAR(30), MIN(t.StartTime)) + ' and ' + CONVERT(NVARCHAR(30), MAX(t.StartTime))
														+ ' that lasted on average '
															+ CONVERT(NVARCHAR(10), AVG(DATEDIFF(SECOND, t.StartTime, t.EndTime)))
																+ ' seconds.' AS Details
						FROM #fnTraceGettable AS t
						WHERE t.EventClass IN (94, 95)
						GROUP BY t.DatabaseName
						HAVING AVG(DATEDIFF(SECOND, t.StartTime, t.EndTime)) > 5;
				
				END;

			IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 215 )
						AND @TraceFileIssue = 0
                        AND EXISTS (SELECT * FROM sys.all_columns WHERE name = 'database_id' AND object_id = OBJECT_ID('sys.dm_exec_sessions'))
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 215) WITH NOWAIT
						
						  SET @StringToExecute = 'INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
                                      [DatabaseName] ,
									  [Details] )

								SELECT	215 AS CheckID ,
										100 AS Priority ,
										''Performance'' AS FindingsGroup ,
										''Implicit Transactions'' AS Finding ,
										DB_NAME(s.database_id) AS DatabaseName,
										N''The database '' +
										DB_NAME(s.database_id)
										+ '' has ''
										+ CONVERT(NVARCHAR(20), COUNT_BIG(*))
										+ '' open implicit transactions ''
										+ '' with an oldest begin time of ''
										+ CONVERT(NVARCHAR(30), MIN(tat.transaction_begin_time)) AS details
								FROM    sys.dm_tran_active_transactions AS tat
								LEFT JOIN sys.dm_tran_session_transactions AS tst
								ON tst.transaction_id = tat.transaction_id
								LEFT JOIN sys.dm_exec_sessions AS s
								ON s.session_id = tst.session_id
								WHERE tat.name = ''implicit_transaction''
								GROUP BY DB_NAME(s.database_id), transaction_type, transaction_state;';


							IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
							IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
							EXECUTE(@StringToExecute);


				
				END;

						
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 216 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 216) WITH NOWAIT;
						
							WITH reboot_airhorn
							    AS
							     (
							         SELECT create_date
							         FROM   sys.databases
							         WHERE  database_id = 2
							         UNION ALL
							         SELECT CAST(DATEADD(SECOND, ( ms_ticks / 1000 ) * ( -1 ), GETDATE()) AS DATETIME)
							         FROM   sys.dm_os_sys_info
							     )
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)														
							SELECT 216 AS CheckID,
							       10 AS Priority,
							       'Recent Restart' AS FindingsGroup,
							       'Server restarted in last 24 hours' AS Finding,
							       'Your server was last restarted on: ' + CONVERT(VARCHAR(30), MAX(reboot_airhorn.create_date)) AS details
							FROM   reboot_airhorn
							HAVING MAX(reboot_airhorn.create_date) >= DATEADD(HOUR, -24, GETDATE());						
						

					END;



				IF @CheckUserDatabaseObjects = 1
					BEGIN

					IF @Debug IN (1, 2) RAISERROR('Starting @CheckUserDatabaseObjects section.', 0, 1) WITH NOWAIT

                        				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 99 )
					        BEGIN
						
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 99) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];  IF EXISTS (SELECT * FROM  sys.tables WITH (NOLOCK) WHERE name = ''sysmergepublications'' ) IF EXISTS ( SELECT * FROM sysmergepublications WITH (NOLOCK) WHERE retention = 0)   INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, Details) SELECT DISTINCT 99, DB_NAME(), 110, ''Performance'', ''Infinite merge replication metadata retention period'', (''The ['' + DB_NAME() + ''] database has merge replication metadata retention period set to infinite - this can be the case of significant performance issues.'')';
					        END;
				        

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 163 )
                            AND EXISTS(SELECT * FROM sys.all_objects WHERE name = 'database_query_store_options')
							BEGIN
								

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 163) WITH NOWAIT;

								EXEC dbo.sp_MSforeachdb 'USE [?];
			                            INSERT INTO #SQLCheckResults
			                            (CheckID,
			                            DatabaseName,
			                            Priority,
			                            FindingsGroup,
			                            Finding,
			                            Details)
		                              SELECT TOP 1 163,
		                              N''?'',
		                              200,
		                              ''Performance'',
		                              ''Query Store Disabled'',
		                              (''The new SQL Server 2016 Query Store feature has not been enabled on this database.'')
		                              FROM [?].sys.database_query_store_options WHERE desired_state = 0
									  AND N''?'' NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''DWConfiguration'', ''DWDiagnostics'', ''DWQueue'', ''ReportServer'', ''ReportServerTempDB'') OPTION (RECOMPILE)';
							END;

						
							IF @ProductVersionMajor >= 13 AND @ProductVersionMinor < 2149 --CU1 has the fix in it
							AND NOT EXISTS ( SELECT  1
											 FROM    #SkipChecks
											 WHERE   DatabaseName IS NULL AND CheckID = 182 )
							AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Enterprise%'
							AND CAST(SERVERPROPERTY('edition') AS VARCHAR(100)) NOT LIKE '%Developer%'
							BEGIN
			
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 182) WITH NOWAIT;
							
							SET @StringToExecute = 'INSERT INTO #SQLCheckResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													Details)
													SELECT TOP 1
													182,
													''Server'',
													20,
													''Reliability'',
													''Query Store Cleanup Disabled'',
													(''SQL 2016 RTM has a bug involving dumps that happen every time Query Store cleanup jobs run. This is fixed in CU1 and later: https://sqlserverupdates.com/sql-server-2016-updates/'')
													FROM    sys.databases AS d
													WHERE   d.is_query_store_on = 1 OPTION (RECOMPILE);';
							
							IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
							IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
							
							EXECUTE(@StringToExecute);
							END;

				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 41 )
					        BEGIN
						
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 41) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'use [?];
		                              INSERT INTO #SQLCheckResults
		                              (CheckID,
		                              DatabaseName,
		                              Priority,
		                              FindingsGroup,
		                              Finding,
		                              Details)
		                              SELECT 41,
		                              N''?'',
		                              170,
		                              ''File Configuration'',
		                              ''Multiple Log Files on One Drive'',
		                              (''The ['' + DB_NAME() + ''] database has multiple log files on the '' + LEFT(physical_name, 1) + '' drive. This is not a performance booster because log file access is sequential, not parallel.'')
		                              FROM [?].sys.database_files WHERE type_desc = ''LOG''
			                            AND N''?'' <> ''[tempdb]''
		                              GROUP BY LEFT(physical_name, 1)
		                              HAVING COUNT(*) > 1 OPTION (RECOMPILE);';
					        END;

				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 42 )
					        BEGIN
						
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 42) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'use [?];
			                            INSERT INTO #SQLCheckResults
			                            (CheckID,
			                            DatabaseName,
			                            Priority,
			                            FindingsGroup,
			                            Finding,
			                            Details)
			                            SELECT DISTINCT 42,
			                            N''?'',
			                            170,
			                            ''File Configuration'',
			                            ''Uneven File Growth Settings in One Filegroup'',
			                            (''The ['' + DB_NAME() + ''] database has multiple data files in one filegroup, but they are not all set up to grow in identical amounts.  This can lead to uneven file activity inside the filegroup.'')
			                            FROM [?].sys.database_files
			                            WHERE type_desc = ''ROWS''
			                            GROUP BY data_space_id
			                            HAVING COUNT(DISTINCT growth) > 1 OR COUNT(DISTINCT is_percent_growth) > 1 OPTION (RECOMPILE);';
					        END;

				            IF NOT EXISTS ( SELECT  1
								            FROM    #SkipChecks
								            WHERE   DatabaseName IS NULL AND CheckID = 82 )
					            BEGIN

									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 82) WITH NOWAIT;

						            EXEC sp_MSforeachdb 'use [?];
		                                INSERT INTO #SQLCheckResults
		                                (CheckID,
		                                DatabaseName,
		                                Priority,
		                                FindingsGroup,
		                                Finding,
		                                Details)
		                                SELECT  DISTINCT 82 AS CheckID,
		                                N''?'' as DatabaseName,
		                                170 AS Priority,
		                                ''File Configuration'' AS FindingsGroup,
		                                ''File growth set to percent'',
		                                ''The ['' + DB_NAME() + ''] database file '' + f.physical_name + '' has grown to '' + CONVERT(NVARCHAR(10), CONVERT(NUMERIC(38, 2), (f.size / 128.) / 1024.)) + '' GB, and is using percent filegrowth settings. This can lead to slow performance during growths if Instant File Initialization is not enabled.''
		                                FROM    [?].sys.database_files f
		                                WHERE   is_percent_growth = 1 and size > 128000  OPTION (RECOMPILE);';
					            END;

                           
				            IF NOT EXISTS ( SELECT  1
								            FROM    #SkipChecks
								            WHERE   DatabaseName IS NULL AND CheckID = 158 )
					            BEGIN

									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 158) WITH NOWAIT;

						            EXEC sp_MSforeachdb 'use [?];
		                                INSERT INTO #SQLCheckResults
		                                (CheckID,
		                                DatabaseName,
		                                Priority,
		                                FindingsGroup,
		                                Finding,
		                                Details)
		                                SELECT  DISTINCT 158 AS CheckID,
		                                N''?'' as DatabaseName,
		                                170 AS Priority,
		                                ''File Configuration'' AS FindingsGroup,
		                                ''File growth set to 1MB'',
		                                ''The ['' + DB_NAME() + ''] database file '' + f.physical_name + '' is using 1MB filegrowth settings, but it has grown to '' + CAST((f.size * 8 / 1000000) AS NVARCHAR(10)) + '' GB. Time to up the growth amount.''
		                                FROM    [?].sys.database_files f
                                        WHERE is_percent_growth = 0 and growth=128 and size > 128000  OPTION (RECOMPILE);';
					            END;

				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 33 )
					        BEGIN
						        IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
							        AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
							        BEGIN
								
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 33) WITH NOWAIT;
										
										EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #SQLCheckResults
					                                (CheckID,
					                                DatabaseName,
					                                Priority,
					                                FindingsGroup,
					                                Finding,
					                                Details)
		                                  SELECT DISTINCT 33,
		                                  db_name(),
		                                  200,
		                                  ''Licensing'',
		                                  ''Enterprise Edition Features In Use'',
		                                  (''The ['' + DB_NAME() + ''] database is using '' + feature_name + ''.  If this database is restored onto a Standard Edition server, the restore will fail on versions prior to 2016 SP1.'')
		                                  FROM [?].sys.dm_db_persisted_sku_features OPTION (RECOMPILE);';
							        END;
					        END;

				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 19 )
					        BEGIN
						        
						
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 19) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
								        ( CheckID ,
								          DatabaseName ,
								          Priority ,
								          FindingsGroup ,
								          Finding ,
								          Details
								        )

								        SELECT  19 AS CheckID ,
										        [name] AS DatabaseName ,
										        200 AS Priority ,
										        'Informational' AS FindingsGroup ,
										        'Replication In Use' AS Finding ,
										        ( 'Database [' + [name]
										          + '] is a replication publisher, subscriber, or distributor.' ) AS Details
								        FROM    sys.databases
								        WHERE   name NOT IN ( SELECT DISTINCT
																        DatabaseName
													          FROM      #SkipChecks
													          WHERE CheckID IS NULL OR CheckID = 19)
										        AND is_published = 1
										        OR is_subscribed = 1
										        OR is_merge_published = 1
										        OR is_distributor = 1;

						       
						        EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #SQLCheckResults
										        (CheckID,
										        DatabaseName,
										        Priority,
										        FindingsGroup,
										        Finding,
										        Details)
							          SELECT DISTINCT 19,
							          db_name(),
							          200,
							          ''Informational'',
							          ''Replication In Use'',
							          (''['' + DB_NAME() + ''] has MSreplication_objects tables in it, indicating it is a replication subscriber.'')
							          FROM [?].sys.tables
							          WHERE name = ''dbo.MSreplication_objects'' AND ''?'' <> ''master'' OPTION (RECOMPILE)';

					        END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 32 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 32) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #SQLCheckResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			Details)
			SELECT 32,
			N''?'',
			150,
			''Performance'',
			''Triggers on Tables'',
			(''The ['' + DB_NAME() + ''] database has '' + CAST(SUM(1) AS NVARCHAR(50)) + '' triggers.'')
			FROM [?].sys.triggers t INNER JOIN [?].sys.objects o ON t.parent_id = o.object_id
			INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE t.is_ms_shipped = 0 AND DB_NAME() != ''ReportServer''
			HAVING SUM(1) > 0 OPTION (RECOMPILE)';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 38 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 38) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #SQLCheckResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			Details)
		  SELECT DISTINCT 38,
		  N''?'',
		  110,
		  ''Performance'',
		  ''Active Tables Without Clustered Indexes'',
		  (''The ['' + DB_NAME() + ''] database has heaps - tables without a clustered index - that are being actively queried.'')
		  FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id
		  INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		  INNER JOIN sys.databases sd ON sd.name = N''?''
		  LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id
		  WHERE i.type_desc = ''HEAP'' AND COALESCE(ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates) IS NOT NULL
		  AND sd.name <> ''tempdb'' AND sd.name <> ''DWDiagnostics'' AND o.is_ms_shipped = 0 AND o.type <> ''S'' OPTION (RECOMPILE)';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 164 )
                            AND EXISTS(SELECT * FROM sys.all_objects WHERE name = 'fn_validate_plan_guide')
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 164) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #SQLCheckResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			Details)
		  SELECT DISTINCT 164,
		  N''?'',
		  20,
		  ''Reliability'',
		  ''Plan Guides Failing'',
		  (''The ['' + DB_NAME() + ''] database has plan guides that are no longer valid, so the queries involved may be failing silently.'')
		  FROM [?].sys.plan_guides g CROSS APPLY fn_validate_plan_guide(g.plan_guide_id) OPTION (RECOMPILE)';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 39 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 39) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #SQLCheckResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			Details)
		  SELECT DISTINCT 39,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Inactive Tables Without Clustered Indexes'',
		  (''The ['' + DB_NAME() + ''] database has heaps - tables without a clustered index - that have not been queried since the last restart.  These may be backup tables carelessly left behind.'')
		  FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id
		  INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		  INNER JOIN sys.databases sd ON sd.name = N''?''
		  LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id
		  WHERE i.type_desc = ''HEAP'' AND COALESCE(ius.user_seeks, ius.user_scans, ius.user_lookups, ius.user_updates) IS NULL
		  AND sd.name <> ''tempdb'' AND sd.name <> ''DWDiagnostics'' AND o.is_ms_shipped = 0 AND o.type <> ''S'' OPTION (RECOMPILE)';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 46 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 46) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT 46,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Leftover Fake Indexes From Wizards'',
		  (''The index ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is a leftover hypothetical index from the Index Tuning Wizard or Database Tuning Advisor.  This index is not actually helping performance and should be removed.'')
		  from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_hypothetical = 1 OPTION (RECOMPILE);';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 47 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 47) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT 47,
		  N''?'',
		  100,
		  ''Performance'',
		  ''Indexes Disabled'',
		  (''The index ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is disabled.  This index is not actually helping performance and should either be enabled or removed.'')
		  from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_disabled = 1 OPTION (RECOMPILE);';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 48 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 48) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT DISTINCT 48,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Foreign Keys Not Trusted'',
		  (''The ['' + DB_NAME() + ''] database has foreign keys that were probably disabled, data was changed, and then the key was enabled again.  Simply enabling the key is not enough for the optimizer to use this key - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'')
		  from [?].sys.foreign_keys i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0 AND N''?'' NOT IN (''master'', ''model'', ''msdb'', ''ReportServer'', ''ReportServerTempDB'') OPTION (RECOMPILE);';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 56 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 56) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT 56,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Check Constraint Not Trusted'',
		  (''The check constraint ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is not trusted - meaning, it was disabled, data was changed, and then the constraint was enabled again.  Simply enabling the constraint is not enough for the optimizer to use this constraint - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'')
		  from [?].sys.check_constraints i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id
		  INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0 OPTION (RECOMPILE);';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 95 )
							BEGIN
								IF @@VERSION NOT LIKE '%Microsoft SQL Server 2000%'
									AND @@VERSION NOT LIKE '%Microsoft SQL Server 2005%'
									BEGIN
										
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 95) WITH NOWAIT;
										
										EXEC dbo.sp_MSforeachdb 'USE [?];
			INSERT INTO #SQLCheckResults
				  (CheckID,
				  DatabaseName,
				  Priority,
				  FindingsGroup,
				  Finding,
				  Details)
			SELECT TOP 1 95 AS CheckID,
			N''?'' as DatabaseName,
			110 AS Priority,
			''Performance'' AS FindingsGroup,
			''Plan Guides Enabled'' AS Finding,
			(''Database ['' + DB_NAME() + ''] has query plan guides so a query will always get a specific execution plan. If you are having trouble getting query performance to improve, it might be due to a frozen plan. Review the DMV sys.plan_guides to learn more about the plan guides in place on this server.'') AS Details
			FROM [?].sys.plan_guides WHERE is_disabled = 0 OPTION (RECOMPILE);';
									END;
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 60 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 60) WITH NOWAIT;
								
								EXEC sp_MSforeachdb 'USE [?];
		  INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT 60 AS CheckID,
		  N''?'' as DatabaseName,
		  100 AS Priority,
		  ''Performance'' AS FindingsGroup,
		  ''Fill Factor Changed'',
		  ''The ['' + DB_NAME() + ''] database has '' + CAST(SUM(1) AS NVARCHAR(50)) + '' objects with fill factor = '' + CAST(fill_factor AS NVARCHAR(5)) + ''%. This can cause memory and storage performance problems, but may also prevent page splits.''
		  FROM    [?].sys.indexes
		  WHERE   fill_factor <> 0 AND fill_factor < 80 AND is_disabled = 0 AND is_hypothetical = 0
		  GROUP BY fill_factor OPTION (RECOMPILE);';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 78 )
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 78) WITH NOWAIT;
								
								EXECUTE master.sys.sp_MSforeachdb 'USE [?];
                                    INSERT INTO #Recompile
                                    SELECT DISTINCT DBName = DB_Name(), SPName = SO.name, SM.is_recompiled, ISR.SPECIFIC_SCHEMA
                                    FROM sys.sql_modules AS SM
                                    LEFT OUTER JOIN master.sys.databases AS sDB ON SM.object_id = DB_id()
                                    LEFT OUTER JOIN dbo.sysobjects AS SO ON SM.object_id = SO.id and type = ''P''
                                    LEFT OUTER JOIN INFORMATION_SCHEMA.ROUTINES AS ISR on ISR.Routine_Name = SO.name AND ISR.SPECIFIC_CATALOG = DB_Name()
                                    WHERE SM.is_recompiled=1  OPTION (RECOMPILE); 
                                    ';
                                INSERT INTO #SQLCheckResults
													(Priority,
													FindingsGroup,
                                                    Finding,
                                                    DatabaseName,
                                                    Details,
                                                    CheckID)
                                SELECT [Priority] = '100',
                                    FindingsGroup = 'Performance',
                                    Finding = 'Stored Procedure WITH RECOMPILE',
                                    DatabaseName = DBName,
                                    Details = '[' + DBName + '].[' + SPSchema + '].[' + ProcName + '] has WITH RECOMPILE in the stored procedure code, which may cause increased CPU usage due to constant recompiles of the code.',
                                    CheckID = '78'
                                FROM #Recompile AS TR WHERE ProcName NOT LIKE 'sp_sql_server_health_check%';
                                DROP TABLE #Recompile;
                            END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 86 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 86) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, Details) SELECT DISTINCT 86, DB_NAME(), 230, ''Security'', ''Elevated Permissions on a Database'', (''In ['' + DB_NAME() + ''], user ['' + u.name + '']  has the role ['' + g.name + ''].  This user can perform tasks beyond just reading and writing data.'') FROM (SELECT memberuid = convert(int, member_principal_id), groupuid = convert(int, role_principal_id) FROM [?].sys.database_role_members) m inner join [?].dbo.sysusers u on m.memberuid = u.uid inner join sysusers g on m.groupuid = g.uid where u.name <> ''dbo'' and g.name in (''db_owner'' , ''db_accessadmin'' , ''db_securityadmin'' , ''db_ddladmin'') OPTION (RECOMPILE);';
							END;

							

										IF NOT EXISTS ( SELECT  1
														FROM    #SkipChecks
														WHERE   DatabaseName IS NULL AND CheckID = 72 )
											BEGIN
												
												IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 72) WITH NOWAIT;
												
												EXEC dbo.sp_MSforeachdb 'USE [?];
								insert into #partdb(dbname, objectname, type_desc)
								SELECT distinct db_name(DB_ID()) as DBName,o.name Object_Name,ds.type_desc
								FROM sys.objects AS o JOIN sys.indexes AS i ON o.object_id = i.object_id
								JOIN sys.data_spaces ds on ds.data_space_id = i.data_space_id
								LEFT OUTER JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id AND s.database_id = DB_ID()
								WHERE  o.type = ''u''
								 -- Clustered and Non-Clustered indexes
								AND i.type IN (1, 2)
								AND o.object_id in
								  (
									SELECT a.object_id from
									  (SELECT ob.object_id, ds.type_desc from sys.objects ob JOIN sys.indexes ind on ind.object_id = ob.object_id join sys.data_spaces ds on ds.data_space_id = ind.data_space_id
									  GROUP BY ob.object_id, ds.type_desc ) a group by a.object_id having COUNT (*) > 1
								  )  OPTION (RECOMPILE);';
												INSERT  INTO #SQLCheckResults
														( CheckID ,
														  DatabaseName ,
														  Priority ,
														  FindingsGroup ,
														  Finding ,
														  Details
														)
														SELECT DISTINCT
																72 AS CheckID ,
																dbname AS DatabaseName ,
																100 AS Priority ,
																'Performance' AS FindingsGroup ,
																'The partitioned database ' + dbname
																+ ' may have non-aligned indexes' AS Finding ,
																'Having non-aligned indexes on partitioned tables may cause inefficient query plans and CPU pressure' AS Details
														FROM    #partdb
														WHERE   dbname IS NOT NULL
																AND dbname NOT IN ( SELECT DISTINCT
																						  DatabaseName
																					FROM  #SkipChecks
																					WHERE CheckID IS NULL OR CheckID = 72);
												DROP TABLE #partdb;
											END;

					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 113 )
									BEGIN
							
							  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 113) WITH NOWAIT;
							
							  EXEC dbo.sp_MSforeachdb 'USE [?];
							  INSERT INTO #SQLCheckResults
									(CheckID,
									DatabaseName,
									Priority,
									FindingsGroup,
									Finding,
									Details)
							  SELECT DISTINCT 113,
							  N''?'',
							  50,
							  ''Reliability'',
							  ''Full Text Indexes Not Updating'',
							  (''At least one full text index in this database has not been crawled in the last week.'')
							  from [?].sys.fulltext_indexes i WHERE change_tracking_state_desc <> ''AUTO'' AND i.is_enabled = 1 AND i.crawl_end_date < DATEADD(dd, -7, GETDATE())  OPTION (RECOMPILE);';
												END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 115 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 115) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?];
		  INSERT INTO #SQLCheckResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				Details)
		  SELECT 115,
		  N''?'',
		  110,
		  ''Performance'',
		  ''Parallelism Rocket Surgery'',
		  (''['' + DB_NAME() + ''] has a make_parallel function, indicating that an advanced developer may be manhandling SQL Server into forcing queries to go parallel.'')
		  from [?].INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = ''make_parallel'' AND ROUTINE_TYPE = ''FUNCTION'' OPTION (RECOMPILE);';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 122 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 122) WITH NOWAIT;
								
								
								IF EXISTS (SELECT *
									  FROM sys.all_columns c
									  INNER JOIN sys.all_objects o ON c.object_id = o.object_id
									  WHERE c.name = 'is_temporary' AND o.name = 'stats')
										
										EXEC dbo.sp_MSforeachdb 'USE [?];
												INSERT INTO #SQLCheckResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													Details)
												SELECT TOP 1 122,
												N''?'',
												200,
												''Performance'',
												''User-Created Statistics In Place'',
												(''['' + DB_NAME() + ''] has '' + CAST(SUM(1) AS NVARCHAR(10)) + '' user-created statistics. This indicates that someone is being a rocket scientist with the stats, and might actually be slowing things down, especially during stats updates.'')
												from [?].sys.stats WHERE user_created = 1 AND is_temporary = 0
                                                HAVING SUM(1) > 0  OPTION (RECOMPILE);';

									ELSE
										EXEC dbo.sp_MSforeachdb 'USE [?];
												INSERT INTO #SQLCheckResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													Details)
												SELECT 122,
												N''?'',
												200,
												''Performance'',
												''User-Created Statistics In Place'',
												(''['' + DB_NAME() + ''] has '' + CAST(SUM(1) AS NVARCHAR(10)) + '' user-created statistics. This indicates that someone is being a rocket scientist with the stats, and might actually be slowing things down, especially during stats updates.'')
												from [?].sys.stats WHERE user_created = 1
                                                HAVING SUM(1) > 0 OPTION (RECOMPILE);';

							END; 

				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 69 )
					        BEGIN
						        IF @ProductVersionMajor >= 11

							        BEGIN
								
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d] (2012 version of Log Info).', 0, 1, 69) WITH NOWAIT;
										
										EXEC sp_MSforeachdb N'USE [?];
		                                      INSERT INTO #LogInfo2012
		                                      EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';
		                                      IF    @@ROWCOUNT > 999
		                                      BEGIN
			                                    INSERT  INTO #SQLCheckResults
			                                    ( CheckID
			                                    ,DatabaseName
			                                    ,Priority
			                                    ,FindingsGroup
			                                    ,Finding
			                                    ,Details)
			                                    SELECT      69
			                                    ,DB_NAME()
			                                    ,170
			                                    ,''File Configuration''
			                                    ,''High VLF Count''
			                                    ,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''
			                                    FROM #LogInfo2012
			                                    WHERE EXISTS (SELECT name FROM master.sys.databases
					                                    WHERE source_database_id is null)  OPTION (RECOMPILE);
		                                      END
		                                    TRUNCATE TABLE #LogInfo2012;';
								        DROP TABLE #LogInfo2012;
							        END;
						        ELSE
							        BEGIN
								
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d] (pre-2012 version of Log Info).', 0, 1, 69) WITH NOWAIT;
										
										EXEC sp_MSforeachdb N'USE [?];
		                                      INSERT INTO #LogInfo
		                                      EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';
		                                      IF    @@ROWCOUNT > 999
		                                      BEGIN
			                                    INSERT  INTO #SQLCheckResults
			                                    ( CheckID
			                                    ,DatabaseName
			                                    ,Priority
			                                    ,FindingsGroup
			                                    ,Finding
			                                    ,Details)
			                                    SELECT      69
			                                    ,DB_NAME()
			                                    ,170
			                                    ,''File Configuration''
			                                    ,''High VLF Count''
			                                    ,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''
			                                    FROM #LogInfo
			                                    WHERE EXISTS (SELECT name FROM master.sys.databases
			                                    WHERE source_database_id is null) OPTION (RECOMPILE);
		                                      END
		                                      TRUNCATE TABLE #LogInfo;';
								        DROP TABLE #LogInfo;
							        END;
					        END;

				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 80 )
					        BEGIN
						
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 80) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding,Details) SELECT DISTINCT 80, DB_NAME(), 170, ''Reliability'', ''Max File Size Set'', (''The ['' + DB_NAME() + ''] database file '' + name + '' has a max file size set to '' + CAST(CAST(max_size AS BIGINT) * 8 / 1024 AS VARCHAR(100)) + ''MB. If it runs out of space, the database will stop working even though there may be drive space available.'') FROM sys.database_files WHERE max_size <> 268435456 AND max_size <> -1 AND type <> 2 AND name <> ''DWDiagnostics''  OPTION (RECOMPILE);';
					        END;

	
						
				        IF NOT EXISTS ( SELECT  1
								        FROM    #SkipChecks
								        WHERE   DatabaseName IS NULL AND CheckID = 74 ) 
					        BEGIN
								TRUNCATE TABLE #TemporaryDatabaseResults;
						
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 74) WITH NOWAIT;
								
								EXEC dbo.sp_MSforeachdb 'USE [?]; IF EXISTS(SELECT * FROM sys.indexes WHERE type IN (5,6))
								 INSERT INTO #TemporaryDatabaseResults (DatabaseName, Finding) VALUES (DB_NAME(), ''Yup'') OPTION (RECOMPILE);';
								IF EXISTS (SELECT * FROM #TemporaryDatabaseResults) SET @ColumnStoreIndexesInUse = 1;
					        END;

						
				        IF EXISTS ( SELECT * FROM sys.all_objects WHERE [name] = 'database_scoped_configurations' )
					        BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d] through [%d].', 0, 1, 194, 197) WITH NOWAIT;
								
								INSERT INTO #DatabaseScopedConfigurationDefaults (configuration_id, [name], 
								default_value, default_value_for_secondary, CheckID)
									SELECT 1, 'MAXDOP', 0, NULL, 194
									UNION ALL
									SELECT 2, 'LEGACY_CARDINALITY_ESTIMATION', 0, NULL, 195
									UNION ALL
									SELECT 3, 'PARAMETER_SNIFFING', 1, NULL, 196
									UNION ALL
									SELECT 4, 'QUERY_OPTIMIZER_HOTFIXES', 0, NULL, 197;
						        EXEC dbo.sp_MSforeachdb 'USE [?]; INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding,Details)
									SELECT def1.CheckID, DB_NAME(), 210, ''Non-Default Database Scoped Config'', dsc.[name],(''Set value: '' + COALESCE(CAST(dsc.value AS NVARCHAR(100)),''Empty'') + '' Default: '' + COALESCE(CAST(def1.default_value AS NVARCHAR(100)),''Empty'') + '' Set value for secondary: '' + COALESCE(CAST(dsc.value_for_secondary AS NVARCHAR(100)),''Empty'') + '' Default value for secondary: '' + COALESCE(CAST(def1.default_value_for_secondary AS NVARCHAR(100)),''Empty''))
									FROM [?].sys.database_scoped_configurations dsc
									INNER JOIN #DatabaseScopedConfigurationDefaults def1 ON dsc.configuration_id = def1.configuration_id
									LEFT OUTER JOIN #DatabaseScopedConfigurationDefaults def ON dsc.configuration_id = def.configuration_id AND (dsc.value = def.default_value OR dsc.value IS NULL) AND (dsc.value_for_secondary = def.default_value_for_secondary OR dsc.value_for_secondary IS NULL)
									LEFT OUTER JOIN #SkipChecks sk ON (sk.CheckID IS NULL OR def.CheckID = sk.CheckID) AND (sk.DatabaseName IS NULL OR sk.DatabaseName = DB_NAME())
									WHERE def.configuration_id IS NULL AND sk.CheckID IS NULL ORDER BY 1
									 OPTION (RECOMPILE);';
			END;

						IF NOT EXISTS (
					SELECT 1
					FROM #SkipChecks
					WHERE DatabaseName IS NULL
						AND CheckID = 218
					)
			BEGIN
				IF @Debug IN (1,2)
				BEGIN
					RAISERROR ('Running CheckId [%d].',0,1,218) WITH NOWAIT;
				END

				EXECUTE sp_MSforeachdb 'USE [?];
					INSERT INTO #SQLCheckResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, Details)
					SELECT 218 AS CheckID
						,''?'' AS DatabaseName
						,150 AS Priority
						,''Performance'' AS FindingsGroup
						,''Objects created with dangerous SET Options'' AS Finding
						,''The '' + QUOTENAME(DB_NAME())
							+ '' database has '' + CONVERT(VARCHAR(20),COUNT(1))
							+ '' objects that were created with dangerous ANSI_NULL or QUOTED_IDENTIFIER options.''
							+ '' These objects can break when using filtered indexes, indexed views''
							+ '' and other advanced SQL features.'' AS Details
					FROM sys.sql_modules sm
					JOIN sys.objects o ON o.[object_id] = sm.[object_id]
						AND (
							sm.uses_ansi_nulls <> 1
							OR sm.uses_quoted_identifier <> 1
							)
						AND o.is_ms_shipped = 0
					HAVING COUNT(1) > 0;';
			END; 

		END;

		IF @CheckProcedureCache = 1
					
					BEGIN

					IF @Debug IN (1, 2) RAISERROR('Begin checking procedure cache', 0, 1) WITH NOWAIT;
					
					BEGIN

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 35 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 35) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT  35 AS CheckID ,
												100 AS Priority ,
												'Performance' AS FindingsGroup ,
												'Single-Use Plans in Procedure Cache' AS Finding ,
												( CAST(COUNT(*) AS VARCHAR(10))
												  + ' query plans are taking up memory in the procedure cache. This may be wasted memory if we cache plans for queries that never get called again. This may be a good use case for SQL Server 2008''s Optimize for Ad Hoc or for Forced Parameterization.' ) AS Details
										FROM    sys.dm_exec_cached_plans AS cp
										WHERE   cp.usecounts = 1
												AND cp.objtype = 'Adhoc'
												AND EXISTS ( SELECT
																  1
															 FROM sys.configurations
															 WHERE
																  name = 'optimize for ad hoc workloads'
																  AND value_in_use = 0 )
										HAVING  COUNT(*) > 1;
							END;

		  						IF @@VERSION LIKE '%Microsoft SQL Server 2005%'
							BEGIN
								IF @CheckProcedureCacheFilter = 'CPU'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			  FROM sys.dm_exec_query_stats qs
			  ORDER BY qs.total_worker_time DESC)
			  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			  FROM queries qs
			  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
			  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

								IF @CheckProcedureCacheFilter = 'Reads'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_logical_reads DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

								IF @CheckProcedureCacheFilter = 'ExecCount'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.execution_count DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

								IF @CheckProcedureCacheFilter = 'Duration'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			FROM sys.dm_exec_query_stats qs
			ORDER BY qs.total_elapsed_time DESC)
			INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			FROM queries qs
			LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
			WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

							END;
						IF @ProductVersionMajor >= 10
							BEGIN
								IF @CheckProcedureCacheFilter = 'CPU'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_worker_time DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

								IF @CheckProcedureCacheFilter = 'Reads'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_logical_reads DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

								IF @CheckProcedureCacheFilter = 'ExecCount'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.execution_count DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

								IF @CheckProcedureCacheFilter = 'Duration'
									OR @CheckProcedureCacheFilter IS NULL
									BEGIN
										SET @StringToExecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_elapsed_time DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
										EXECUTE(@StringToExecute);
									END;

		
								UPDATE  #dm_exec_query_stats
								SET     query_plan_filtered = qp.query_plan
								FROM    #dm_exec_query_stats qs
										CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle,
																  qs.statement_start_offset,
																  qs.statement_end_offset)
										AS qp;

							END;

		
						UPDATE  #dm_exec_query_stats
						SET     query_plan = qp.query_plan ,
								[text] = st.[text] ,
								text_filtered = SUBSTRING(st.text,
														  ( qs.statement_start_offset
															/ 2 ) + 1,
														  ( ( CASE qs.statement_end_offset
																WHEN -1
																THEN DATALENGTH(st.text)
																ELSE qs.statement_end_offset
															  END
															  - qs.statement_start_offset )
															/ 2 ) + 1)
						FROM    #dm_exec_query_stats qs
								CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
								CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle)
								AS qp;

		
						DELETE  #dm_exec_query_stats
						WHERE   text LIKE '%sp_sql_server_health_check%'
								OR text LIKE '%#SQLCheckResults%';

		

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 63 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 63) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  63 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Implicit Conversion' AS Finding ,
												( 'One of the top resource-intensive queries is comparing two fields that are not the same datatype.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%CONVERT_IMPLICIT%'
												AND COALESCE(qs.query_plan_filtered,
															 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%PhysicalOp="Index Scan"%';
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 64 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 64) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  64 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Implicit Conversion Affecting Cardinality' AS Finding ,
												( 'One of the top resource-intensive queries has an implicit conversion that is affecting cardinality estimation.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT_IMPLICIT%';
							END;

							
							IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 118 )
								BEGIN
									
									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 118) WITH NOWAIT;
									
									INSERT  INTO #SQLCheckResults
											( CheckID ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  Details ,
											  QueryPlan ,
											  QueryPlanFiltered
											)
											SELECT  118 AS CheckID ,
													120 AS Priority ,
													'Query Plans' AS FindingsGroup ,
													'RID or Key Lookups' AS Finding ,
													'One of the top resource-intensive queries contains RID or Key Lookups. Try to avoid them by creating covering indexes.' AS Details ,
													qs.query_plan ,
													qs.query_plan_filtered
											FROM    #dm_exec_query_stats qs
											WHERE   COALESCE(qs.query_plan_filtered,
															 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%Lookup="1"%';
								END; 

						
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 65 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 65) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  65 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Missing Index' AS Finding ,
												( 'One of the top resource-intensive queries may be dramatically improved by adding an index.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%MissingIndexGroup%';
							END;

						
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 66 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 66) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  66 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Cursor' AS Finding ,
												( 'One of the top resource-intensive queries is using a cursor.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<StmtCursor%';
							END;

		

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 67 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 67) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details ,
										  QueryPlan ,
										  QueryPlanFiltered
										)
										SELECT  67 AS CheckID ,
												120 AS Priority ,
												'Query Plans' AS FindingsGroup ,
												'Scalar UDFs' AS Finding ,
												( 'One of the top resource-intensive queries is using a user-defined scalar function that may inhibit parallelism.' ) AS Details ,
												qs.query_plan ,
												qs.query_plan_filtered
										FROM    #dm_exec_query_stats qs
										WHERE   COALESCE(qs.query_plan_filtered,
														 CAST(qs.query_plan AS NVARCHAR(MAX))) LIKE '%<UserDefinedFunction%';
							END;

					END; 
				END;
									
		
		IF @ProductVersionMajor >= 10
								AND NOT EXISTS ( SELECT 1
								FROM #SkipChecks
								WHERE DatabaseName IS NULL AND CheckID = 187 )

		IF SERVERPROPERTY('IsHadrEnabled') = 1
    		BEGIN

				IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 187) WITH NOWAIT;
				
				INSERT    INTO [#SQLCheckResults]
                               	( [CheckID] ,
                                [Priority] ,
                                [FindingsGroup] ,
                                [Finding] ,
                                [Details] )
               	SELECT
                        187 AS [CheckID] ,
                        230 AS [Priority] ,
                        'Security' AS [FindingsGroup] ,
                        'Endpoints Owned by Users' AS [Finding] ,
                       	( 'Endpoint ' + ep.[name] + ' is owned by ' + SUSER_NAME(ep.principal_id) + '. If the endpoint owner login is disabled or not available due to Active Directory problems, the high availability will stop working.'
                        ) AS [Details]
					FROM sys.database_mirroring_endpoints ep
					LEFT OUTER JOIN sys.dm_server_services s ON SUSER_NAME(ep.principal_id) = s.service_account
					WHERE s.service_account IS NULL AND ep.principal_id <> 1;
    		END;

		
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 68 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 68) WITH NOWAIT;
						
						EXEC sp_MSforeachdb N'USE [?];
						INSERT #DBCCs
							(ParentObject,
							Object,
							Field,
							Value)
						EXEC (''DBCC DBInfo() With TableResults, NO_INFOMSGS'');
						UPDATE #DBCCs SET DbName = N''?'' WHERE DbName IS NULL OPTION (RECOMPILE);';

						WITH    DB2
								  AS ( SELECT DISTINCT
												Field ,
												Value ,
												DbName
									   FROM     #DBCCs
                                       INNER JOIN sys.databases d ON #DBCCs.DbName = d.name
									   WHERE    Field = 'dbi_dbccLastKnownGood'
                                         AND d.create_date < DATEADD(dd, -14, GETDATE())
									 )
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  DatabaseName ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  68 AS CheckID ,
											DB2.DbName AS DatabaseName ,
											1 AS PRIORITY ,
											'Reliability' AS FindingsGroup ,
											'Last good DBCC CHECKDB over 2 weeks old' AS Finding ,
											'Last successful CHECKDB: '
											+ CASE DB2.Value
												WHEN '1900-01-01 00:00:00.000'
												THEN ' never.'
												ELSE DB2.Value
											  END AS Details
									FROM    DB2
									WHERE   DB2.DbName <> 'tempdb'
											AND DB2.DbName NOT IN ( SELECT DISTINCT
																  DatabaseName
																FROM
																  #SkipChecks
																WHERE CheckID IS NULL OR CheckID = 68)
											AND DB2.DbName NOT IN ( SELECT  name
                                                                    FROM    sys.databases
                                                                    WHERE   is_read_only = 1)
											AND CONVERT(DATETIME, DB2.Value, 121) < DATEADD(DD,
																  -14,
																  CURRENT_TIMESTAMP);
					END;

	
			IF NOT EXISTS ( SELECT  1
							FROM    #SkipChecks
							WHERE   DatabaseName IS NULL AND CheckID = 70 )
				BEGIN
					IF @@SERVERNAME IS NULL
						BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 70) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  70 AS CheckID ,
											200 AS Priority ,
											'Informational' AS FindingsGroup ,
											'@@Servername Not Set' AS Finding ,
											'@@Servername variable is null. You can fix it by executing: "sp_addserver ''<LocalServerName>'', local"' AS Details;
						END;

					IF  
						(@@SERVERNAME IS NOT NULL
						AND
						
						CHARINDEX('\',CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))) = 0
						AND
						
						SERVERPROPERTY('IsClustered') = 0
						AND
						
						@@SERVERNAME <> CAST(ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'),@@SERVERNAME) AS NVARCHAR(128)) )
						 BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 70) WITH NOWAIT;
							
							INSERT  INTO #SQLCheckResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  Details
									)
									SELECT  70 AS CheckID ,
											200 AS Priority ,
											'Configuration' AS FindingsGroup ,
											'@@Servername Not Correct' AS Finding ,
											'The @@Servername is different than the computer name, which may trigger certificate errors.' AS Details;
						END;

				END;
		
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 73 )
					BEGIN

						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 73) WITH NOWAIT;
						
						DECLARE @AlertInfo TABLE
							(
							  FailSafeOperator NVARCHAR(255) ,
							  NotificationMethod INT ,
							  ForwardingServer NVARCHAR(255) ,
							  ForwardingSeverity INT ,
							  PagerToTemplate NVARCHAR(255) ,
							  PagerCCTemplate NVARCHAR(255) ,
							  PagerSubjectTemplate NVARCHAR(255) ,
							  PagerSendSubjectOnly NVARCHAR(255) ,
							  ForwardAlways INT
							);
						INSERT  INTO @AlertInfo
								EXEC [master].[dbo].[sp_MSgetalertinfo] @includeaddresses = 0;
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  73 AS CheckID ,
										200 AS Priority ,
										'Monitoring' AS FindingsGroup ,
										'No failsafe operator configured' AS Finding ,
										( 'No failsafe operator is configured on this server.  This is a good idea just in-case there are issues with the [msdb] database that prevents alerting.' ) AS Details
								FROM    @AlertInfo
								WHERE   FailSafeOperator IS NULL;
					END;


				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 74 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 74) WITH NOWAIT;
						
						INSERT  INTO #TraceStatus
								EXEC ( ' DBCC TRACESTATUS(-1) WITH NO_INFOMSGS'
									);
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  74 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'TraceFlag On' AS Finding ,
										'Trace flag ' +
										CASE WHEN [T].[TraceFlag] = '2330' THEN ' 2330 enabled globally. Using this trace Flag disables missing index requests!'
											 WHEN [T].[TraceFlag] = '1211' THEN ' 1211 enabled globally. Using this Trace Flag disables lock escalation when you least expect it. No Bueno!'
											 WHEN [T].[TraceFlag] = '1224' THEN ' 1224 enabled globally. Using this Trace Flag disables lock escalation based on the number of locks being taken. You shouldn''t have done that, Dave.'
											 WHEN [T].[TraceFlag] = '652'  THEN ' 652 enabled globally. Using this Trace Flag disables pre-fetching during index scans. If you hate slow queries, you should turn that off.'
											 WHEN [T].[TraceFlag] = '661'  THEN ' 661 enabled globally. Using this Trace Flag disables ghost record removal. Who you gonna call? No one, turn that thing off.'
											 WHEN [T].[TraceFlag] = '1806'  THEN ' 1806 enabled globally. Using this Trace Flag disables Instant File Initialization. I question your sanity.'
											 WHEN [T].[TraceFlag] = '3505'  THEN ' 3505 enabled globally. Using this Trace Flag disables Checkpoints. Probably not the wisest idea.'
											 WHEN [T].[TraceFlag] = '8649'  THEN ' 8649 enabled globally. Using this Trace Flag drops cost threshold for parallelism down to 0. I hope this is a dev server.'
										     WHEN [T].[TraceFlag] = '834' AND @ColumnStoreIndexesInUse = 1 THEN ' 834 is enabled globally. Using this Trace Flag with Columnstore Indexes is not a great idea.'
											 WHEN [T].[TraceFlag] = '8017' AND (CAST(SERVERPROPERTY('Edition') AS NVARCHAR(1000)) LIKE N'%Express%') THEN ' 8017 is enabled globally, which is the default for express edition.'
                                             WHEN [T].[TraceFlag] = '8017' AND (CAST(SERVERPROPERTY('Edition') AS NVARCHAR(1000)) NOT LIKE N'%Express%') THEN ' 8017 is enabled globally. Using this Trace Flag disables creation schedulers for all logical processors. Not good.'
											 ELSE [T].[TraceFlag] + ' is enabled globally.' END
										AS Details
								FROM    #TraceStatus T;
					END;

			IF @ProductVersionMajor >= 11 AND NOT EXISTS ( SELECT 1
									FROM   #SkipChecks
									WHERE  DatabaseName IS NULL AND CheckID = 162 )
				BEGIN
							
					IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 162) WITH NOWAIT;

					INSERT  INTO #SQLCheckResults
							( CheckID ,
								Priority ,
								FindingsGroup ,
								Finding ,
								Details
							)
							SELECT  162 AS CheckID ,
									50 AS Priority ,
									'Performance' AS FindingGroup ,
									'Poison Wait Detected: CMEMTHREAD & NUMA'  AS Finding ,
									CONVERT(VARCHAR(10), (MAX([wait_time_ms]) / 1000) / 86400) + ':' + CONVERT(VARCHAR(20), DATEADD(s, (MAX([wait_time_ms]) / 1000), 0), 108) + ' of this wait have been recorded'
                                    + CASE WHEN ts.status = 1 THEN ' despite enabling trace flag 8048 already.'
                                        ELSE '. In servers with over 8 cores per NUMA node, when CMEMTHREAD waits are a bottleneck, trace flag 8048 may be needed.'
                                    END
							FROM sys.dm_os_nodes n
							INNER JOIN sys.[dm_os_wait_stats] w ON w.wait_type = 'CMEMTHREAD'
                            LEFT OUTER JOIN #TraceStatus ts ON ts.TraceFlag = 8048 AND ts.status = 1
							WHERE n.node_id = 0 AND n.online_scheduler_count >= 8
								AND EXISTS (SELECT * FROM sys.dm_os_nodes WHERE node_id > 0 AND node_state_desc NOT LIKE '%DAC')
							GROUP BY w.wait_type, ts.status
							HAVING SUM([wait_time_ms]) > (SELECT 5000 * datediff(HH,create_date,CURRENT_TIMESTAMP) AS hours_since_startup FROM sys.databases WHERE name='tempdb')
							AND SUM([wait_time_ms]) > 60000;
				END;


		
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 75 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 75) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  75 AS CheckID ,
										DB_NAME(a.database_id) ,
										50 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'Transaction Log Larger than Data File' AS Finding ,
										'The database [' + DB_NAME(a.database_id)
										+ '] has a ' + CAST((CAST(a.size AS BIGINT) * 8 / 1000000) AS NVARCHAR(20)) + ' GB transaction log file, larger than the total data file sizes. This may indicate that transaction log backups are not being performed or not performed often enough.' AS Details
								FROM    sys.master_files a
								WHERE   a.type = 1
										AND DB_NAME(a.database_id) NOT IN (
										SELECT DISTINCT
												DatabaseName
										FROM    #SkipChecks
										WHERE CheckID = 75 OR CheckID IS NULL)
										AND a.size > 125000 
										AND a.size > ( SELECT   SUM(CAST(b.size AS BIGINT))
													   FROM     sys.master_files b
													   WHERE    a.database_id = b.database_id
																AND b.type = 0
													 )
										AND a.database_id IN (
										SELECT  database_id
										FROM    sys.databases
										WHERE   source_database_id IS NULL );
					END;

		
				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 76 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 76) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  76 AS CheckID ,
										name AS DatabaseName ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Collation is ' + collation_name AS Finding ,
										'Collation differences between user databases and tempdb can cause conflicts especially when comparing string values' AS Details
								FROM    sys.databases
							WHERE   name NOT IN ( 'master', 'model', 'msdb')
										AND name NOT LIKE 'ReportServer%'
										AND name NOT IN ( SELECT DISTINCT
																  DatabaseName
														  FROM    #SkipChecks
														  WHERE CheckID IS NULL OR CheckID = 76)
										AND collation_name <> ( SELECT
																  collation_name
																FROM
																  sys.databases
																WHERE
																  name = 'tempdb'
															  );
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 77 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 77) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  DatabaseName ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  77 AS CheckID ,
										dSnap.[name] AS DatabaseName ,
										50 AS Priority ,
										'Reliability' AS FindingsGroup ,
										'Database Snapshot Online' AS Finding ,
										'Database [' + dSnap.[name]
										+ '] is a snapshot of ['
										+ dOriginal.[name]
										+ ']. Make sure you have enough drive space to maintain the snapshot as the original database grows.' AS Details
								FROM    sys.databases dSnap
										INNER JOIN sys.databases dOriginal ON dSnap.source_database_id = dOriginal.database_id
																  AND dSnap.name NOT IN (
																  SELECT DISTINCT DatabaseName
																  FROM #SkipChecks
																  WHERE CheckID = 77 OR CheckID IS NULL);
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 79 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 79) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  79 AS CheckID ,
										
										
                						CASE WHEN (cast(datediff(dd, substring(cast(sjh.run_date as nvarchar(10)), 1, 4) + '-' + substring(cast(sjh.run_date as nvarchar(10)), 5, 2) + '-' + substring(cast(sjh.run_date as nvarchar(10)), 7, 2), GETDATE()) AS INT) < 30) OR (j.[enabled] = 1 AND ssc.[enabled] = 1 )THEN
                						    100
                						ELSE -- no job history (implicit) AND job not run in the past 30 days AND (Job disabled OR Job Schedule disabled)
            						        200
                						END AS Priority,
										'Performance' AS FindingsGroup ,
										'Shrink Database Job' AS Finding ,
										'In the [' + j.[name] + '] job, step ['
										+ step.[step_name]
										+ '] has SHRINKDATABASE or SHRINKFILE, which may be causing database fragmentation.'
										+ CASE WHEN COALESCE(ssc.name,'0') != '0' THEN + ' (Schedule: [' + ssc.name + '])' ELSE + '' END AS Details
								FROM    msdb.dbo.sysjobs j
										INNER JOIN msdb.dbo.sysjobsteps step ON j.job_id = step.job_id
										LEFT OUTER JOIN msdb.dbo.sysjobschedules AS sjsc
										    ON j.job_id = sjsc.job_id
										LEFT OUTER JOIN msdb.dbo.sysschedules AS ssc
										    ON sjsc.schedule_id = ssc.schedule_id
										    AND sjsc.job_id = j.job_id
										LEFT OUTER JOIN msdb.dbo.sysjobhistory AS sjh
										    ON j.job_id = sjh.job_id
										    AND step.step_id = sjh.step_id
										    AND sjh.run_date IN (SELECT max(sjh2.run_date) FROM msdb.dbo.sysjobhistory AS sjh2 WHERE sjh2.job_id = j.job_id) -- get the latest entry date
										    AND sjh.run_time IN (SELECT max(sjh3.run_time) FROM msdb.dbo.sysjobhistory AS sjh3 WHERE sjh3.job_id = j.job_id AND sjh3.run_date = sjh.run_date) -- get the latest entry time
								WHERE   step.command LIKE N'%SHRINKDATABASE%'
										OR step.command LIKE N'%SHRINKFILE%';
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 81 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 81) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT  81 AS CheckID ,
										200 AS Priority ,
										'Non-Active Server Config' AS FindingsGroup ,
										cr.name AS Finding ,
										( 'This sp_configure option isn''t running under its set value.  Its set value is '
										  + CAST(cr.[value] AS VARCHAR(100))
										  + ' and its running value is '
										  + CAST(cr.value_in_use AS VARCHAR(100))
										  + '. When someone does a RECONFIGURE or restarts the instance, this setting will start taking effect.' ) AS Details
								FROM    sys.configurations cr
								WHERE   cr.value <> cr.value_in_use
                                 AND NOT (cr.name = 'min server memory (MB)' AND cr.value IN (0,16) AND cr.value_in_use IN (0,16));
					END;

				IF NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 123 )
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 123) WITH NOWAIT;
						
						INSERT  INTO #SQLCheckResults
								( CheckID ,
								  Priority ,
								  FindingsGroup ,
								  Finding ,
								  Details
								)
								SELECT TOP 1 123 AS CheckID ,
										200 AS Priority ,
										'Informational' AS FindingsGroup ,
										'Agent Jobs Starting Simultaneously' AS Finding ,
										( 'Multiple SQL Server Agent jobs are configured to start simultaneously' ) AS Details
								FROM    msdb.dbo.sysjobactivity
								WHERE start_execution_date > DATEADD(dd, -14, GETDATE())
								GROUP BY start_execution_date HAVING COUNT(*) > 1;
					END;

				IF @CheckServerInfo = 1
					BEGIN

IF @ProductVersionMajor >= 10
			   AND NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 172 )
					BEGIN
					-- sys.dm_os_host_info includes both Windows and Linux info
					IF EXISTS (SELECT 1
									FROM	sys.all_objects
									WHERE	name = 'dm_os_host_info' )
					BEGIN

						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 172) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )

							SELECT
							172 AS [CheckID] ,
							250 AS [Priority] ,
							'Server Info' AS [FindingsGroup] ,
							'Operating System Version' AS [Finding] ,
							( CASE
								WHEN [ohi].[host_platform] = 'Linux' THEN 'You''re running the ' + CAST([ohi].[host_distribution] AS VARCHAR(35)) + ' distribution of ' + CAST([ohi].[host_platform] AS VARCHAR(35)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								WHEN [ohi].[host_platform] = 'Windows' AND [ohi].[host_release] = '5' THEN 'You''re running a really old version: Windows 2000, version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								WHEN [ohi].[host_platform] = 'Windows' AND [ohi].[host_release] > '5' AND [ohi].[host_release] < '6' THEN 'You''re running a really old version: ' + CAST([ohi].[host_distribution] AS VARCHAR(50)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								WHEN [ohi].[host_platform] = 'Windows' AND [ohi].[host_release] >= '6' AND [ohi].[host_release] <= '6.1' THEN 'You''re running a pretty old version: Windows: ' + CAST([ohi].[host_distribution] AS VARCHAR(50)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								WHEN [ohi].[host_platform] = 'Windows' AND [ohi].[host_release] = '6.2' THEN 'You''re running a rather modern version of Windows: ' + CAST([ohi].[host_distribution] AS VARCHAR(50)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								WHEN [ohi].[host_platform] = 'Windows' AND [ohi].[host_release] = '6.3' THEN 'You''re running a pretty modern version of Windows: ' + CAST([ohi].[host_distribution] AS VARCHAR(50)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								WHEN [ohi].[host_platform] = 'Windows' AND [ohi].[host_release] > '6.3' THEN 'Hot dog! You''re living in the future! You''re running ' + CAST([ohi].[host_distribution] AS VARCHAR(50)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								ELSE 'You''re running ' + CAST([ohi].[host_distribution] AS VARCHAR(35)) + ', version ' + CAST([ohi].[host_release] AS VARCHAR(5))
								END
							   ) AS [Details]
							 FROM [sys].[dm_os_host_info] [ohi];
					END;
					ELSE
					BEGIN
					

						IF EXISTS ( SELECT  1
												FROM    sys.all_objects
												WHERE   name = 'dm_os_windows_info' )

							BEGIN
						
								  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 172) WITH NOWAIT;
						
								  INSERT    INTO [#SQLCheckResults]
											( [CheckID] ,
											  [Priority] ,
											  [FindingsGroup] ,
											  [Finding] ,
											  [Details] )

									SELECT
									172 AS [CheckID] ,
									250 AS [Priority] ,
									'Server Info' AS [FindingsGroup] ,
									'Windows Version' AS [Finding] ,
									( CASE
										WHEN [owi].[windows_release] = '5' THEN 'You''re running a really old version: Windows 2000, version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										WHEN [owi].[windows_release] > '5' AND [owi].[windows_release] < '6' THEN 'You''re running a really old version: Windows Server 2003/2003R2 era, version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										WHEN [owi].[windows_release] >= '6' AND [owi].[windows_release] <= '6.1' THEN 'You''re running a pretty old version: Windows: Server 2008/2008R2 era, version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										WHEN [owi].[windows_release] = '6.2' THEN 'You''re running a rather modern version of Windows: Server 2012 era, version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										WHEN [owi].[windows_release] = '6.3' THEN 'You''re running a pretty modern version of Windows: Server 2012R2 era, version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										WHEN [owi].[windows_release] = '10.0' THEN 'You''re running a pretty modern version of Windows: Server 2016 era, version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										ELSE 'Hot dog! You''re living in the future! You''re running version ' + CAST([owi].[windows_release] AS VARCHAR(5))
										END
									   ) AS [Details]
									 FROM [sys].[dm_os_windows_info] [owi];

							END;
						END;
					END;


IF @ProductVersionMajor >= 10 AND  NOT EXISTS ( SELECT  1
								FROM    #SkipChecks
								WHERE   DatabaseName IS NULL AND CheckID = 166 )
					BEGIN
						
						  IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 166) WITH NOWAIT;
						
						  INSERT    INTO [#SQLCheckResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
									  [Details] )
							SELECT
							166 AS [CheckID] ,
							250 AS [Priority] ,
							'Server Info' AS [FindingsGroup] ,
							'Locked Pages In Memory Enabled' AS [Finding] ,
							( 'You currently have '
							  + CASE WHEN [dopm].[locked_page_allocations_kb] / 1024. / 1024. > 0
									 THEN CAST([dopm].[locked_page_allocations_kb] / 1024 / 1024 AS VARCHAR(100))
										  + ' GB'
									 ELSE CAST([dopm].[locked_page_allocations_kb] / 1024 AS VARCHAR(100))
										  + ' MB'
								END + ' of pages locked in memory.' ) AS [Details]
						  FROM
							[sys].[dm_os_process_memory] AS [dopm]
						  WHERE
							[dopm].[locked_page_allocations_kb] > 0;
					END;

			
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 166 )
							AND EXISTS ( SELECT  *
											FROM    sys.all_objects o
													INNER JOIN sys.all_columns c ON o.object_id = c.object_id
											WHERE   o.name = 'dm_os_sys_info'
													AND c.name = 'sql_memory_model' )
							BEGIN
										
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 166) WITH NOWAIT;
										
										SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			SELECT  166 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Memory Model Unconventional'' AS Finding ,
			''Memory Model: '' + CAST(sql_memory_model_desc AS NVARCHAR(100))
			FROM sys.dm_os_sys_info WHERE sql_memory_model <> 1 OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';

										EXECUTE(@StringToExecute);
									END;

			
					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 184 )
							AND (@ProductVersionMajor >= 13) OR (@ProductVersionMajor = 12 AND @ProductVersionMinor >= 5000)
						BEGIN
							
							IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 184) WITH NOWAIT;
							
							INSERT INTO #ErrorLog
							EXEC sys.xp_readerrorlog 0, 1, N'Database Instant File Initialization: enabled';

							IF @@ROWCOUNT > 0
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  [Priority] ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT
												193 AS [CheckID] ,
												250 AS [Priority] ,
												'Server Info' AS [FindingsGroup] ,
												'Instant File Initialization Enabled' AS [Finding] ,
												'The service account has the Perform Volume Maintenance Tasks permission.';
						END;

			
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 192 )
							AND EXISTS ( SELECT  *
											FROM    sys.all_objects o
													INNER JOIN sys.all_columns c ON o.object_id = c.object_id
											WHERE   o.name = 'dm_server_services'
													AND c.name = 'instant_file_initialization_enabled' )
							BEGIN
										
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 192) WITH NOWAIT;
										
										SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
			SELECT  192 AS CheckID ,
			50 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Instant File Initialization Not Enabled'' AS Finding ,
			''Consider enabling IFI for faster restores and data file growths.''
			FROM sys.dm_server_services WHERE instant_file_initialization_enabled <> ''Y'' AND filename LIKE ''%sqlservr.exe%'' OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
									END;

					IF NOT EXISTS ( SELECT  1
									FROM    #SkipChecks
									WHERE   DatabaseName IS NULL AND CheckID = 130 )
						BEGIN
									
									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 130) WITH NOWAIT;
									
									INSERT  INTO #SQLCheckResults
											( CheckID ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  Details
											)
											SELECT  130 AS CheckID ,
													250 AS Priority ,
													'Server Info' AS FindingsGroup ,
													'Server Name' AS Finding ,
													@@SERVERNAME AS Details
												WHERE @@SERVERNAME IS NOT NULL;
								END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 83 )
							BEGIN
								IF EXISTS ( SELECT  *
											FROM    sys.all_objects
											WHERE   name = 'dm_server_services' )
									BEGIN
										
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 83) WITH NOWAIT;
										
				
				
										SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
				SELECT  83 AS CheckID ,
				250 AS Priority ,
				''Server Info'' AS FindingsGroup ,
				''Services'' AS Finding ,
				N''Service: '' + servicename + N'' runs under service account '' + service_account + N''. Last startup time: '' + COALESCE(CAST(CASE WHEN YEAR(last_startup_time) <= 1753 THEN CAST(''17530101'' as datetime) ELSE CAST(last_startup_time AS DATETIME) END AS VARCHAR(50)), ''not shown.'') + ''. Startup type: '' + startup_type_desc + N'', currently '' + status_desc + ''.''
				FROM sys.dm_server_services OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
									END;
							END;

			
						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 84 )
							BEGIN
								IF EXISTS ( SELECT  *
											FROM    sys.all_objects o
													INNER JOIN sys.all_columns c ON o.object_id = c.object_id
											WHERE   o.name = 'dm_os_sys_info'
													AND c.name = 'physical_memory_kb' )
									BEGIN
										
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 84) WITH NOWAIT;
										
										SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding,Details)
			SELECT  84 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Hardware'' AS Finding ,
			''Logical processors: '' + CAST(cpu_count AS VARCHAR(50)) + ''. Physical memory: '' + CAST( CAST(ROUND((physical_memory_kb / 1024.0 / 1024), 1) AS INT) AS VARCHAR(50)) + ''GB.''
			FROM sys.dm_os_sys_info OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
									END;

			
								IF EXISTS ( SELECT  *
											FROM    sys.all_objects o
													INNER JOIN sys.all_columns c ON o.object_id = c.object_id
											WHERE   o.name = 'dm_os_sys_info'
													AND c.name = 'physical_memory_in_bytes' )
									BEGIN
										
										IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 84) WITH NOWAIT;
										
										SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding,Details)
			SELECT  84 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Hardware'' AS Finding ,
			''Logical processors: '' + CAST(cpu_count AS VARCHAR(50)) + ''. Physical memory: '' + CAST( CAST(ROUND((physical_memory_in_bytes / 1024.0 / 1024 / 1024), 1) AS INT) AS VARCHAR(50)) + ''GB.''
			FROM sys.dm_os_sys_info OPTION (RECOMPILE);';
										
										IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
										IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
										
										EXECUTE(@StringToExecute);
									END;
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 85 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 85) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT  85 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'SQL Server Service' AS Finding ,
												N'Version: '
												+ CAST(SERVERPROPERTY('productversion') AS NVARCHAR(100))
												+ N'. Patch Level: '
												+ CAST(SERVERPROPERTY('productlevel') AS NVARCHAR(100))
								  				+ CASE WHEN SERVERPROPERTY('ProductUpdateLevel') IS NULL
												       THEN N''
												       ELSE N'. Cumulative Update: '
													   + CAST(SERVERPROPERTY('ProductUpdateLevel') AS NVARCHAR(100))
												END
												+ N'. Edition: '
												+ CAST(SERVERPROPERTY('edition') AS VARCHAR(100))
												+ N'. Availability Groups Enabled: '
												+ CAST(COALESCE(SERVERPROPERTY('IsHadrEnabled'),
																0) AS VARCHAR(100))
												+ N'. Availability Groups Manager Status: '
												+ CAST(COALESCE(SERVERPROPERTY('HadrManagerStatus'),
																0) AS VARCHAR(100));
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 88 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 88) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT  88 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'SQL Server Last Restart' AS Finding ,
												CAST(create_date AS VARCHAR(100))
										FROM    sys.databases
										WHERE   database_id = 2;
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 91 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 91) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT  91 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'Server Last Restart' AS Finding ,
												CAST(DATEADD(SECOND, (ms_ticks/1000)*(-1), GETDATE()) AS nvarchar(25))
										FROM sys.dm_os_sys_info;
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 92 )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 92) WITH NOWAIT;
								
								INSERT  INTO #driveInfo
										( drive, SIZE )
										EXEC master..xp_fixeddrives;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT  92 AS CheckID ,
												250 AS Priority ,
												'Server Info' AS FindingsGroup ,
												'Drive ' + i.drive + ' Space' AS Finding ,
												CAST(i.SIZE AS VARCHAR(30))
												+ 'MB free on ' + i.drive
												+ ' drive' AS Details
										FROM    #driveInfo AS i;
								DROP TABLE #driveInfo;
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 103 )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
												INNER JOIN sys.all_columns c ON o.object_id = c.object_id
										 WHERE  o.name = 'dm_os_sys_info'
												AND c.name = 'virtual_machine_type_desc' )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 103) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding, Details)
									SELECT 103 AS CheckID,
									250 AS Priority,
									''Server Info'' AS FindingsGroup,
									''Virtual Server'' AS Finding,
											''Type: ('' + virtual_machine_type_desc + '')'' AS Details
									FROM sys.dm_os_sys_info
									WHERE virtual_machine_type <> 0 OPTION (RECOMPILE);';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 214 )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
												INNER JOIN sys.all_columns c ON o.object_id = c.object_id
										 WHERE  o.name = 'dm_os_sys_info'
												AND c.name = 'container_type_desc' )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 214) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding,Details)
									SELECT 214 AS CheckID,
									250 AS Priority,
									''Server Info'' AS FindingsGroup,
									''Container'' AS Finding,
									''Type: ('' + container_type_desc + '')'' AS Details
									FROM sys.dm_os_sys_info
									WHERE container_type_desc <> ''NONE'' OPTION (RECOMPILE);';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 114 )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
										 WHERE  o.name = 'dm_os_memory_nodes' )
							AND EXISTS ( SELECT *
										 FROM   sys.all_objects o
										 INNER JOIN sys.all_columns c ON o.object_id = c.object_id
										 WHERE  o.name = 'dm_os_nodes'
                                	 		AND c.name = 'processor_group' )
							BEGIN
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 114) WITH NOWAIT;
								
								SET @StringToExecute = 'INSERT INTO #SQLCheckResults (CheckID, Priority, FindingsGroup, Finding,Details)
										SELECT  114 AS CheckID ,
												250 AS Priority ,
												''Server Info'' AS FindingsGroup ,
												''Hardware - NUMA Config'' AS Finding ,
												''Node: '' + CAST(n.node_id AS NVARCHAR(10)) + '' State: '' + node_state_desc
												+ '' Online schedulers: '' + CAST(n.online_scheduler_count AS NVARCHAR(10)) + '' Offline schedulers: '' + CAST(oac.offline_schedulers AS VARCHAR(100)) + '' Processor Group: '' + CAST(n.processor_group AS NVARCHAR(10))
												+ '' Memory node: '' + CAST(n.memory_node_id AS NVARCHAR(10)) + '' Memory VAS Reserved GB: '' + CAST(CAST((m.virtual_address_space_reserved_kb / 1024.0 / 1024) AS INT) AS NVARCHAR(100))
										FROM sys.dm_os_nodes n
										INNER JOIN sys.dm_os_memory_nodes m ON n.memory_node_id = m.memory_node_id
										OUTER APPLY (SELECT
										COUNT(*) AS [offline_schedulers]
										FROM sys.dm_os_schedulers dos
										WHERE n.node_id = dos.parent_node_id
										AND dos.status = ''VISIBLE OFFLINE''
										) oac
										WHERE n.node_state_desc NOT LIKE ''%DAC%''
										ORDER BY n.node_id OPTION (RECOMPILE);';
								
								IF @Debug = 2 AND @StringToExecute IS NOT NULL PRINT @StringToExecute;
								IF @Debug = 2 AND @StringToExecute IS NULL PRINT '@StringToExecute has gone NULL, for some reason.';
								
								EXECUTE(@StringToExecute);
							END;
								

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 211 )
								BEGIN																		
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 211) WITH NOWAIT;

								DECLARE @outval VARCHAR(36);
								
								EXEC master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
														   @key = 'SOFTWARE\Policies\Microsoft\Power\PowerSettings',
														   @value_name = 'ActivePowerScheme',
														   @value = @outval OUTPUT;

								IF @outval IS NULL 
								EXEC master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
								                           @key = 'SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes',
								                           @value_name = 'ActivePowerScheme',
								                           @value = @outval OUTPUT;
														   
								DECLARE @cpu_speed_mhz int,
								        @cpu_speed_ghz decimal(18,2);
								
								EXEC master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
								                           @key = 'HARDWARE\DESCRIPTION\System\CentralProcessor\0',
								                           @value_name = '~MHz',
								                           @value = @cpu_speed_mhz OUTPUT;
								
								SELECT @cpu_speed_ghz = CAST(CAST(@cpu_speed_mhz AS DECIMAL) / 1000 AS DECIMAL(18,2));

									INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)							
							SELECT  211 AS CheckId,
									250 AS Priority,
									'Server Info' AS FindingsGroup,
									'Power Plan' AS Finding,
									'Your server has '
									+ CAST(@cpu_speed_ghz as VARCHAR(4))
									+ 'GHz CPUs, and is in '
									+ CASE @outval
							             WHEN 'a1841308-3541-4fab-bc81-f71556f20b4a'
							             THEN 'power saving mode -- are you sure this is a production SQL Server?'
							             WHEN '381b4222-f694-41f0-9685-ff5bb260df2e'
							             THEN 'balanced power mode -- Uh... you want your CPUs to run at full speed, right?'
							             WHEN '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
							             THEN 'high performance power mode'
										 ELSE 'an unknown power mode.'
							        END AS Details
								
								END;

						IF NOT EXISTS ( SELECT  1
										FROM    #SkipChecks
										WHERE   DatabaseName IS NULL AND CheckID = 212 )
								BEGIN																		
								
								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 212) WITH NOWAIT;

						        INSERT INTO #Instances (Instance_Number, Instance_Name, Data_Field)
								EXEC master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
								                           @key = 'SOFTWARE\Microsoft\Microsoft SQL Server',
								                           @value_name = 'InstalledInstances'
								
                                IF (SELECT COUNT(*) FROM #Instances) > 1
                                BEGIN

                                    DECLARE @InstanceCount NVARCHAR(MAX)
                                    SELECT @InstanceCount = COUNT(*) FROM #Instances

									INSERT INTO #SQLCheckResults
										(
										  CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)							
							        SELECT
									    212 AS CheckId ,
									    250 AS Priority ,
									    'Server Info' AS FindingsGroup ,
									    'Instance Stacking' AS Finding ,
									    'Your Server has ' + @InstanceCount + ' Instances of SQL Server installed. More than one is usually a bad idea'
							    END;
	                        END;
							
							IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 106 )
											AND (select convert(int,value_in_use) from sys.configurations where name = 'default trace enabled' ) = 1
                                AND DATALENGTH( COALESCE( @base_tracefilename, '' ) ) > DATALENGTH('.TRC')
							BEGIN

								IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 106) WITH NOWAIT;
								
								INSERT  INTO #SQLCheckResults
										( CheckID ,
										  Priority ,
										  FindingsGroup ,
										  Finding ,
										  Details
										)
										SELECT
												 106 AS CheckID
												,250 AS Priority
												,'Server Info' AS FindingsGroup
												,'Default Trace Contents' AS Finding
												,'The default trace holds '+cast(DATEDIFF(hour,MIN(StartTime),GETDATE())as VARCHAR(30))+' hours of data'
												+' between '+cast(Min(StartTime) as VARCHAR(30))+' and '+cast(GETDATE()as VARCHAR(30))
												+('. The default trace files are located in: '+left( @curr_tracefilename,len(@curr_tracefilename) - @indx)
												) as Details
										FROM    ::fn_trace_gettable( @base_tracefilename, default )
										WHERE EventClass BETWEEN 65500 and 65600;
							END; 

							IF NOT EXISTS ( SELECT  1
											FROM    #SkipChecks
											WHERE   DatabaseName IS NULL AND CheckID = 152 )
							BEGIN
								IF EXISTS (SELECT * FROM sys.dm_os_wait_stats ws
											LEFT OUTER JOIN #IgnorableWaits i ON ws.wait_type = i.wait_type
											WHERE wait_time_ms > .1 * @CpuMsSinceWaitsCleared AND waiting_tasks_count > 0
											AND i.wait_type IS NULL)
									BEGIN
									
									
									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 152) WITH NOWAIT;
									
									WITH os(wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms)
									AS
									(SELECT ws.wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms, signal_wait_time_ms
										FROM sys.dm_os_wait_stats ws
										LEFT OUTER JOIN #IgnorableWaits i ON ws.wait_type = i.wait_type
											WHERE i.wait_type IS NULL
												AND wait_time_ms > .1 * @CpuMsSinceWaitsCleared
												AND waiting_tasks_count > 0)
									INSERT  INTO #SQLCheckResults
											( CheckID ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  Details
											)
											SELECT TOP 9
													 152 AS CheckID
													,240 AS Priority
													,'Wait Stats' AS FindingsGroup
													, CAST(ROW_NUMBER() OVER(ORDER BY os.wait_time_ms DESC) AS NVARCHAR(10)) + N' - ' + os.wait_type AS Finding
													, Details = CAST(CAST(SUM(os.wait_time_ms / 1000.0 / 60 / 60) OVER (PARTITION BY os.wait_type) AS NUMERIC(18,1)) AS NVARCHAR(20)) + N' hours of waits, ' +
													CAST(CAST((SUM(60.0 * os.wait_time_ms) OVER (PARTITION BY os.wait_type) ) / @MsSinceWaitsCleared  AS NUMERIC(18,1)) AS NVARCHAR(20)) + N' minutes average wait time per hour, ' +
													
													CAST(CAST(
														100. * SUM(os.signal_wait_time_ms) OVER (PARTITION BY os.wait_type)
														/ (1. * SUM(os.wait_time_ms) OVER ())
														AS NUMERIC(18,1)) AS NVARCHAR(40)) + N'% signal wait, ' +
													CAST(SUM(os.waiting_tasks_count) OVER (PARTITION BY os.wait_type) AS NVARCHAR(40)) + N' waiting tasks, ' +
													CAST(CASE WHEN  SUM(os.waiting_tasks_count) OVER (PARTITION BY os.wait_type) > 0
													THEN
														CAST(
															SUM(os.wait_time_ms) OVER (PARTITION BY os.wait_type)
																/ (1. * SUM(os.waiting_tasks_count) OVER (PARTITION BY os.wait_type))
															AS NUMERIC(18,1))
													ELSE 0 END AS NVARCHAR(40)) + N' ms average wait time.'
											FROM    os
											ORDER BY SUM(os.wait_time_ms / 1000.0 / 60 / 60) OVER (PARTITION BY os.wait_type) DESC;
									END;

								
								IF NOT EXISTS (SELECT * FROM #SQLCheckResults WHERE CheckID IN (107, 108, 109, 121, 152, 162))
								BEGIN
									
									IF @Debug IN (1, 2) RAISERROR('Running CheckId [%d].', 0, 1, 153) WITH NOWAIT;
									
									INSERT  INTO #SQLCheckResults
											( CheckID ,
											  Priority ,
											  FindingsGroup ,
											  Finding ,
											  Details
											)
										VALUES (153, 240, 'Wait Stats', 'No Significant Waits Detected', 'This server might be just sitting around idle, or someone may have cleared wait stats recently.');
								END;
							END;

					END; 
			END;

				
				IF @IgnorePrioritiesAbove IS NOT NULL
					DELETE  #SQLCheckResults
					WHERE   [Priority] > @IgnorePrioritiesAbove AND CheckID <> -1;

				IF @IgnorePrioritiesBelow IS NOT NULL
					DELETE  #SQLCheckResults
					WHERE   [Priority] < @IgnorePrioritiesBelow AND CheckID <> -1;

				
				IF @SkipChecksTable IS NOT NULL
					BEGIN
						DELETE  FROM #SQLCheckResults
						WHERE   DatabaseName IN ( SELECT    DatabaseName
												  FROM      #SkipChecks
												  WHERE CheckID IS NULL
												  AND (ServerName IS NULL OR ServerName = SERVERPROPERTY('ServerName')));
						DELETE  FROM #SQLCheckResults
						WHERE   CheckID IN ( SELECT    CheckID
												  FROM      #SkipChecks
												  WHERE DatabaseName IS NULL
												  AND (ServerName IS NULL OR ServerName = SERVERPROPERTY('ServerName')));
						DELETE r FROM #SQLCheckResults r
							INNER JOIN #SkipChecks c ON r.DatabaseName = c.DatabaseName and r.CheckID = c.CheckID
												  AND (ServerName IS NULL OR ServerName = SERVERPROPERTY('ServerName'));
					END;

				
				IF @SummaryMode > 0
					BEGIN
					UPDATE #SQLCheckResults
					  SET Finding = br.Finding + ' (' + CAST(brTotals.recs AS NVARCHAR(20)) + ')'
					  FROM #SQLCheckResults br
						INNER JOIN (SELECT FindingsGroup, Finding, Priority, COUNT(*) AS recs FROM #SQLCheckResults GROUP BY FindingsGroup, Finding, Priority) brTotals ON br.FindingsGroup = brTotals.FindingsGroup AND br.Finding = brTotals.Finding AND br.Priority = brTotals.Priority
						WHERE brTotals.recs > 1;

					DELETE br
					  FROM #SQLCheckResults br
					  WHERE EXISTS (SELECT * FROM #SQLCheckResults brLower WHERE br.FindingsGroup = brLower.FindingsGroup AND br.Finding = brLower.Finding AND br.Priority = brLower.Priority AND br.ID > brLower.ID);

					END;

						
				
				DECLARE @ValidOutputServer BIT;
				DECLARE @ValidOutputLocation BIT;
				DECLARE @LinkedServerDBCheck NVARCHAR(2000);
				DECLARE @ValidLinkedServerDB INT;
				DECLARE @tmpdbchk table (cnt int);
				IF @OutputServerName IS NOT NULL
					BEGIN
						
						IF @Debug IN (1, 2) RAISERROR('Outputting to a remote server.', 0, 1) WITH NOWAIT;
						
						IF EXISTS (SELECT server_id FROM sys.servers WHERE QUOTENAME([name]) = @OutputServerName)
							BEGIN
								SET @LinkedServerDBCheck = 'SELECT 1 WHERE EXISTS (SELECT * FROM '+@OutputServerName+'.master.sys.databases WHERE QUOTENAME([name]) = '''+@OutputDatabaseName+''')';
								INSERT INTO @tmpdbchk EXEC sys.sp_executesql @LinkedServerDBCheck;
								SET @ValidLinkedServerDB = (SELECT COUNT(*) FROM @tmpdbchk);
								IF (@ValidLinkedServerDB > 0)
									BEGIN
										SET @ValidOutputServer = 1;
										SET @ValidOutputLocation = 1;
									END;
								ELSE
									RAISERROR('The specified database was not found on the output server', 16, 0);
							END;
						ELSE
							BEGIN
								RAISERROR('The specified output server was not found', 16, 0);
							END;
					END;
				ELSE
					BEGIN
						IF @OutputDatabaseName IS NOT NULL
							AND @OutputSchemaName IS NOT NULL
							AND @OutputTableName IS NOT NULL
							AND EXISTS ( SELECT *
								 FROM   sys.databases
								 WHERE  QUOTENAME([name]) = @OutputDatabaseName)
							BEGIN
								SET @ValidOutputLocation = 1;
							END;
						ELSE IF @OutputDatabaseName IS NOT NULL
							AND @OutputSchemaName IS NOT NULL
							AND @OutputTableName IS NOT NULL
							AND NOT EXISTS ( SELECT *
								 FROM   sys.databases
								 WHERE  QUOTENAME([name]) = @OutputDatabaseName)
							BEGIN
								RAISERROR('The specified output database was not found on this server', 16, 0);
							END;
						ELSE
							BEGIN
								SET @ValidOutputLocation = 0;
							END;
					END;

				
				IF @ValidOutputLocation = 1
					BEGIN
						SET @StringToExecute = 'USE '
							+ @OutputDatabaseName
							+ '; IF EXISTS(SELECT * FROM '
							+ @OutputDatabaseName
							+ '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
							+ @OutputSchemaName
							+ ''') AND NOT EXISTS (SELECT * FROM '
							+ @OutputDatabaseName
							+ '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
							+ @OutputSchemaName + ''' AND QUOTENAME(TABLE_NAME) = '''
							+ @OutputTableName + ''') CREATE TABLE '
							+ @OutputSchemaName + '.'
							+ @OutputTableName
							+ ' (ID INT IDENTITY(1,1) NOT NULL,
								ServerName NVARCHAR(128),
								CheckDate DATETIMEOFFSET,
								Priority TINYINT ,
								FindingsGroup VARCHAR(50) ,
								Finding VARCHAR(200) ,
								DatabaseName NVARCHAR(128),
								Details NVARCHAR(4000) ,
								QueryPlan [XML] NULL ,
								QueryPlanFiltered [NVARCHAR](MAX) NULL,
								CheckID INT ,
								CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));';
						IF @ValidOutputServer = 1
							BEGIN
								SET @StringToExecute = REPLACE(@StringToExecute,''''+@OutputSchemaName+'''',''''''+@OutputSchemaName+'''''');
								SET @StringToExecute = REPLACE(@StringToExecute,''''+@OutputTableName+'''',''''''+@OutputTableName+'''''');
								SET @StringToExecute = REPLACE(@StringToExecute,'[XML]','[NVARCHAR](MAX)');
								EXEC('EXEC('''+@StringToExecute+''') AT ' + @OutputServerName);
							END;
						ELSE
							BEGIN
								EXEC(@StringToExecute);
							END;
						IF @ValidOutputServer = 1
							BEGIN
								SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
								+ @OutputServerName + '.'
								+ @OutputDatabaseName
								+ '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
								+ @OutputSchemaName + ''') INSERT '
								+ @OutputServerName + '.'
								+ @OutputDatabaseName + '.'
								+ @OutputSchemaName + '.'
								+ @OutputTableName
								+ ' (ServerName, CheckDate, CheckID, DatabaseName, Priority, FindingsGroup, Finding,
								 Details, QueryPlan, QueryPlanFiltered) SELECT '''
								+ CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
								+ ''', SYSDATETIMEOFFSET(), CheckID, DatabaseName, Priority, FindingsGroup, Finding,
								 Details, CAST(QueryPlan AS NVARCHAR(MAX)), QueryPlanFiltered 
								 FROM #SQLCheckResults ORDER BY Priority , FindingsGroup , Finding , Details';

								EXEC(@StringToExecute);
							END;
						ELSE
							BEGIN
								SET @StringToExecute = N' IF EXISTS(SELECT * FROM '
								+ @OutputDatabaseName
								+ '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
								+ @OutputSchemaName + ''') INSERT '
								+ @OutputDatabaseName + '.'
								+ @OutputSchemaName + '.'
								+ @OutputTableName
								+ ' (ServerName, CheckDate, CheckID, DatabaseName, Priority,
								 FindingsGroup, Finding,Details, QueryPlan, QueryPlanFiltered) SELECT '''
								+ CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
								+ ''', SYSDATETIMEOFFSET(), CheckID, DatabaseName, Priority,
								 FindingsGroup, Finding, Details, QueryPlan, QueryPlanFiltered 
								 FROM #SQLCheckResults ORDER BY Priority , FindingsGroup , Finding , Details';
								
								EXEC(@StringToExecute);
							END;
					END;
				ELSE IF (SUBSTRING(@OutputTableName, 2, 2) = '##')
					BEGIN
						IF @ValidOutputServer = 1
							BEGIN
								RAISERROR('Due to the nature of temporary tables, outputting to a linked server requires a permanent table.', 16, 0);
							END;
						ELSE
							BEGIN
								SET @StringToExecute = N' IF (OBJECT_ID(''tempdb..'
									+ @OutputTableName
									+ ''') IS NOT NULL) DROP TABLE ' + @OutputTableName + ';'
									+ 'CREATE TABLE '
									+ @OutputTableName
									+ ' (ID INT IDENTITY(1,1) NOT NULL,
										ServerName NVARCHAR(128),
										CheckDate DATETIMEOFFSET,
										Priority TINYINT ,
										FindingsGroup VARCHAR(50) ,
										Finding VARCHAR(200) ,
										DatabaseName NVARCHAR(128),
										Details NVARCHAR(4000) ,
										QueryPlan [XML] NULL ,
										QueryPlanFiltered [NVARCHAR](MAX) NULL,
										CheckID INT ,
										CONSTRAINT [PK_' + CAST(NEWID() AS CHAR(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));'
									+ ' INSERT '
									+ @OutputTableName
									+ ' (ServerName, CheckDate, CheckID, DatabaseName, Priority,
									 FindingsGroup, Finding,  Details, 
									QueryPlan, QueryPlanFiltered) SELECT '''
									+ CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128))
									+ ''', SYSDATETIMEOFFSET(), CheckID, DatabaseName, Priority,
									 FindingsGroup, Finding,Details, 
									QueryPlan, QueryPlanFiltered FROM #SQLCheckResults ORDER BY Priority , 
									FindingsGroup , Finding , Details';
							
									EXEC(@StringToExecute);
							END;
					END;
				ELSE IF (SUBSTRING(@OutputTableName, 2, 1) = '#')
					BEGIN
						RAISERROR('Due to the nature of Dymamic SQL,
						 only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
					END;

				DECLARE @separator AS VARCHAR(1);
				IF @OutputType = 'RSV'
					SET @separator = CHAR(31);
				ELSE
					SET @separator = ',';

				IF @OutputType = 'COUNT'
					BEGIN
						SELECT  COUNT(*) AS Warnings
						FROM    #SQLCheckResults;
					END;
				ELSE
					IF @OutputType IN ( 'CSV', 'RSV' )
						BEGIN

							SELECT  Result = CAST([Priority] AS NVARCHAR(100))
									+ @separator + CAST(CheckID AS NVARCHAR(100))
									+ @separator + COALESCE([FindingsGroup],
															'(N/A)') + @separator
									+ COALESCE([Finding], '(N/A)') + @separator
									+ COALESCE(DatabaseName, '(N/A)') + @separator
									+ COALESCE([Details], '(N/A)')
							FROM    #SQLCheckResults
							ORDER BY Priority ,
									FindingsGroup ,
									Finding ,
									DatabaseName ,
									Details;
						END;
					ELSE IF @OutputXMLasNVARCHAR = 1 AND @OutputType <> 'NONE'
						BEGIN
							SELECT  [Priority] ,
									[FindingsGroup] ,
									[Finding] ,
									[DatabaseName] ,
									[Details] ,
									CAST([QueryPlan] AS NVARCHAR(MAX)) AS QueryPlan,
									[QueryPlanFiltered] ,
									CheckID
							FROM    #SQLCheckResults
							ORDER BY Priority ,
									FindingsGroup ,
									Finding ,
									DatabaseName ,
									Details;
						END;
					ELSE IF @OutputType = 'MARKDOWN'
						BEGIN
							WITH Results AS (SELECT row_number() OVER (ORDER BY Priority, FindingsGroup, Finding, DatabaseName, Details) AS rownum, *
												FROM #SQLCheckResults
												WHERE Priority > 0 AND Priority < 255 AND FindingsGroup IS NOT NULL AND Finding IS NOT NULL
												AND FindingsGroup <> 'Security')
							SELECT
								CASE
									WHEN r.Priority <> COALESCE(rPrior.Priority, 0) OR r.FindingsGroup <> rPrior.FindingsGroup  
									THEN @crlf + N'**Priority ' + CAST(COALESCE(r.Priority,N'') AS NVARCHAR(5)) + N': '
									 + COALESCE(r.FindingsGroup,N'') + N'**:' + @crlf + @crlf
									ELSE N''
								END
								+ CASE WHEN r.Finding <> COALESCE(rPrior.Finding,N'') AND r.Finding <> rNext.Finding THEN N'- '
								 + COALESCE(r.Finding,N'') + N' ' + COALESCE(r.DatabaseName, N'') + N' - ' + COALESCE(r.Details,N'') + @crlf
									   WHEN r.Finding <> COALESCE(rPrior.Finding,N'') AND r.Finding = rNext.Finding 
									   AND r.Details = rNext.Details THEN N'- ' + COALESCE(r.Finding,N'') + N' - ' + COALESCE(r.Details,N'') 
									   + @crlf + @crlf + N'    * ' + COALESCE(r.DatabaseName, N'') + @crlf
									   WHEN r.Finding <> COALESCE(rPrior.Finding,N'') AND r.Finding = rNext.Finding THEN N'- ' 
									   + COALESCE(r.Finding,N'') + @crlf + CASE WHEN r.DatabaseName IS NULL THEN N'' ELSE  N'    * '
									    + COALESCE(r.DatabaseName,N'') END + CASE WHEN r.Details <> rPrior.Details THEN N' - '
									     + COALESCE(r.Details,N'') + @crlf ELSE '' END
									   ELSE CASE WHEN r.DatabaseName IS NULL THEN N'' ELSE  N'    * ' + COALESCE(r.DatabaseName,N'') END
									    + CASE WHEN r.Details <> rPrior.Details THEN N' - ' + COALESCE(r.Details,N'') + @crlf ELSE N'' + @crlf END
								END + @crlf
							  FROM Results r
							  LEFT OUTER JOIN Results rPrior ON r.rownum = rPrior.rownum + 1
							  LEFT OUTER JOIN Results rNext ON r.rownum = rNext.rownum - 1
							ORDER BY r.rownum FOR XML PATH(N'');
						END;
					ELSE IF @OutputType <> 'NONE'
						BEGIN
							
							SELECT  [Priority] ,
									[FindingsGroup] ,
									[Finding] ,
									[DatabaseName] ,
									[Details] ,
									[QueryPlan] ,
									[QueryPlanFiltered] ,
									CheckID
							FROM    #SQLCheckResults
							ORDER BY Priority ,
									FindingsGroup ,
									Finding ,
									DatabaseName ,
									Details;
						END;

				DROP TABLE #SQLCheckResults;

				IF @OutputProcedureCache = 1
				AND @CheckProcedureCache = 1
					SELECT TOP 20
							total_worker_time / execution_count AS AvgCPU ,
							total_worker_time AS TotalCPU ,
							CAST(ROUND(100.00 * total_worker_time
									   / ( SELECT   SUM(total_worker_time)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentCPU ,
							total_elapsed_time / execution_count AS AvgDuration ,
							total_elapsed_time AS TotalDuration ,
							CAST(ROUND(100.00 * total_elapsed_time
									   / ( SELECT   SUM(total_elapsed_time)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentDuration ,
							total_logical_reads / execution_count AS AvgReads ,
							total_logical_reads AS TotalReads ,
							CAST(ROUND(100.00 * total_logical_reads
									   / ( SELECT   SUM(total_logical_reads)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentReads ,
							execution_count ,
							CAST(ROUND(100.00 * execution_count
									   / ( SELECT   SUM(execution_count)
										   FROM     sys.dm_exec_query_stats
										 ), 2) AS MONEY) AS PercentExecutions ,
							CASE WHEN DATEDIFF(mi, creation_time,
											   qs.last_execution_time) = 0 THEN 0
								 ELSE CAST(( 1.00 * execution_count / DATEDIFF(mi,
																  creation_time,
																  qs.last_execution_time) ) AS MONEY)
							END AS executions_per_minute ,
							qs.creation_time AS plan_creation_time ,
							qs.last_execution_time ,
							text ,
							text_filtered ,
							query_plan ,
							query_plan_filtered ,
							sql_handle ,
							query_hash ,
							plan_handle ,
							query_plan_hash
					FROM    #dm_exec_query_stats qs
					ORDER BY CASE UPPER(@CheckProcedureCacheFilter)
							   WHEN 'CPU' THEN total_worker_time
							   WHEN 'READS' THEN total_logical_reads
							   WHEN 'EXECCOUNT' THEN execution_count
							   WHEN 'DURATION' THEN total_elapsed_time
							   ELSE total_worker_time
							 END DESC;

	END;

    SET NOCOUNT OFF;
GO

