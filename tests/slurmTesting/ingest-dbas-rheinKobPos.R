library(ntsportal)
library(logger)
library(glue)
library(testthat)
dirTestResults <- "tests/slurmTesting/testResults-dbas-rheinKobPos"
# After completion
# Remember to delete indices after ingest
logger::log_info("started")
startTime <- Sys.time()
newTableId <- ingestFeatureRecords(dirTestResults)
logger::log_info("ended")
endTime <- Sys.time()

index <- newTableId[[1]]$ntsp26.1_feature_bfg 
features <- getTableByEsql(glue('FROM {index} | KEEP name, filename, batchname, start, station, mz, intensity, esi_ion_spec'))
totalRows <- getNrow(getDbComm(), index)
totalMonths <- nrow(getTableByEsql(glue('FROM {index} | STATS BY month = BUCKET(start, "1 month")'))) 

deleteTable(getDbComm(), index)

expect_equal(pull(features, station)[1], "rhein_ko_l")
expect_type(pull(features, mz), "double")
expect_match(pull(features, start), "\\d{4}-\\d{2}-\\d{2}")
expect_match(pull(features, esi_ion_spec), "_")
expect_equal(nrow(features), 1000)
expect_gte(totalRows, 4376)
expect_equal(totalMonths, 2)

message("Time needed to ingest: ", round(difftime(endTime, startTime, units = "secs")), " s")
