library(logger)
library(ntsportal)
library(glue)
library(testthat)
library(dplyr)

log_info("started")
startTime <- Sys.time()
newTableId <- ingestFeatureRecords("tests/longTestsScreening/testResults-dbasRheinKobPos")
log_info("ended")
endTime <- Sys.time()

index <- newTableId[[1]]$ntsp26.1_feature_bfg  
features <- getTableByEsql(glue('FROM {index} | KEEP name, filename, batchname, start, station, mz, intensity, esi_ion_spec'))
totalRows <- getNrow(getDbComm(), index)
hitsByComp <- getTableByEsql(glue('FROM {index} | STATS ms2_hits = COUNT() WHERE score_ms2_match IS NOT NULL, files = VALUES(filename) BY esi_ion_spec'))

deleteTable(getDbComm(), index)

expect_equal(pull(features, station)[1], "rhein_ko_l")
expect_type(pull(features, mz), "double")
expect_match(pull(features, start), "\\d{4}-\\d{2}-\\d{2}")
expect_match(pull(features, esi_ion_spec), "_")
expect_equal(nrow(features), 1000)
expect_gte(totalRows, 3471)
expect_gte(min(hitsByComp$ms2_hits), 2)

message("Time needed to ingest: ", round(difftime(endTime, startTime, units = "secs")), " s")
