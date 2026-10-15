library(logger)
library(ntsportal)
library(glue)
library(testthat)
library(dplyr)

log_info("started")
startTime <- Sys.time()
newTableId <- ingestFeatureRecords("tests/longTestsScreening/testResults-ntsRheinKobPos")
log_info("ended")
endTime <- Sys.time()
index <- newTableId[[1]]$ntsp26.1_feature_bfg 
features <- getTableByEsql(glue('FROM {index} | KEEP name, filename, batchname, start, station, mz, intensity, esi_ion_spec'))
totalRows <- getNrow(getDbComm(), index)

deleteTable(getDbComm(), index)

expect_equal(pull(features, station)[1], "rhein_ko_l")
expect_type(pull(features, mz), "double")
expect_match(pull(features, start), "\\d{4}-\\d{2}-\\d{2}")
expect_match(pull(features, esi_ion_spec), "_")
expect_equal(nrow(features), 1000)
expect_gte(totalRows, 26117)

message("Time needed to ingest: ", round(difftime(endTime, startTime, units = "secs")), " s")
