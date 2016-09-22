/*

** Author: Tomaz Kastrun
** Web: http://tomaztsql.wordpress.com
** Twitter: @tomaz_tsql
** Created: 22.09.2016; Ljubljana
** xdf (External data frame) file size based on block size when reading through rxImport
** R and T-SQL

*/


USE SQLR;
GO


-- Make sure your path location is pointing to RevoscaleR library folder!
EXECUTE sp_execute_external_script
	  @language = N'R'
	 ,@script = N'library(RevoScaleR) 
				OutputDataSet <- data.frame(rxGetOption("sampleDataDir"))'
WITH RESULT SETS (( 
					path_folder NVARCHAR(1000)
					))

-- check for ComputeContext
DECLARE @RStat NVARCHAR(4000)
SET @RStat = 'library(RevoScaleR)
			 cc <- rxGetOption("computeContext")
			 OutputDataSet <- data.frame(cc@description)';
EXECUTE sp_execute_external_script
	  @language = N'R'
	 ,@script = @RStat
WITH RESULT SETS ((compute_context NVARCHAR(100)))


					
-- change the file
-- test for chunks: 200.000 Rows; 20.000 Rows; 2.000 Rows; 200 Rows
DECLARE @RStat NVARCHAR(4000)
SET @RStat = 'library(RevoScaleR)
		      #rxSetComputeContext("RxLocalSeq")
			  ptm <- proc.time()
			  inFile <- file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv")
			  rxTextToXdf(inFile = inFile, outFile = "AirlineDemoSmall_200000_NC.xdf",  stringsAsFactors = T, rowsPerRead = 200000, overwrite=TRUE)
			  outFile <- file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall_200000_NC.xdf")
			  rxImport(inData = inFile, outFile = outFile, overwrite=TRUE)
			  d <- proc.time() - ptm	
			  OutputDataSet <- data.frame(d[3])';

EXECUTE sp_execute_external_script
	  @language = N'R'
	 ,@script = @RStat
WITH RESULT SETS (( 
					Time_df NVARCHAR(100)
					))



CREATE PROCEDURE rxImport_Test
(
@rowsPerRead INT 
)

AS

BEGIN
	DECLARE @RStat NVARCHAR(4000)
	SET @RStat = 'library(RevoScaleR)
				  #rxSetComputeContext("RxLocalSeq")
				  ptm <- proc.time()
				  inFile <- file.path(rxGetOption("sampleDataDir"), "AirlineDemoSmall.csv")
				  filename <- "AirlineDemoSmall_'+CAST(@rowsPerRead AS VARCHAR(100))+'_TSQL_NC.xdf"
				  rxTextToXdf(inFile = inFile, outFile = filename,  stringsAsFactors = T, rowsPerRead = '+CAST(@rowsPerRead AS VARCHAR(100))+', overwrite=TRUE)
				  outFile <- file.path(rxGetOption("sampleDataDir"), filename)
				  rxImport(inData = inFile, outFile = outFile, overwrite=TRUE)
				  d <- proc.time() - ptm
				  filesize <- data.frame(file.size(filename))	
				  time     <- data.frame(d[3])
				  RowsPerRead <- data.frame('+CAST(@rowsPerRead AS VARCHAR(100))+')
				  filename_xdf <- data.frame(filename)
				  ran <- data.frame(Sys.time())
				  OutputDataSet <- cbind(as.character(filesize), time, RowsPerRead, filename_xdf, ran)';
	EXECUTE sp_execute_external_script
		  @language = N'R'
		 ,@script = @RStat
	WITH RESULT SETS (( 
						 Filesize NVARCHAR(100)
						,Time_df NVARCHAR(100)
						,RowsPerRead NVARCHAR(100)
						,filename_xdf NVARCHAR(100)
						,DateExecute NVARCHAR(100)
						))
END

-- DROP TABLE rxImport_results

CREATE TABLE rxImport_results
( filesize VARCHAR(100)
,time_df VARCHAR(100)
,RowsPerRead VARCHAR(100)
,filename_xdf VARCHAR(100)
,DateExecute VARCHAR(100)
)


INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 2;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 20;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 200;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 2000;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 20000;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 200000;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 2000000;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 20000000;
GO

INSERT INTO rxImport_results
EXEC rxImport_Test @rowsPerRead = 200000000;
GO





--- Check file information

DECLARE @RStat NVARCHAR(4000)
SET @RStat = 'library(RevoScaleR)	
			  info <- rxGetInfoXdf(data="AirlineDemoSmall_20000000_TSQL_NC.xdf", getVarInfo = TRUE)	
			  OutputDataSet <- data.frame(info$numBlocks)';

EXECUTE sp_execute_external_script
	  @language = N'R'
	 ,@script = @RStat
WITH RESULT SETS (( 
					nof_blocks NVARCHAR(100)
					))


/*
AirlineDemoSmall_2_TSQL_NC.xdf
AirlineDemoSmall_20_TSQL_NC.xdf
AirlineDemoSmall_200_TSQL_NC.xdf
AirlineDemoSmall_2000_TSQL_NC.xdf
AirlineDemoSmall_20000_TSQL_NC.xdf
AirlineDemoSmall_200000_TSQL_NC.xdf
AirlineDemoSmall_2000000_TSQL_NC.xdf
AirlineDemoSmall_20000000_TSQL_NC.xdf
AirlineDemoSmall_200000000_TSQL_NC.xdf
*/