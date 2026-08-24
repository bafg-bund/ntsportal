
# nohup Rscript tests/slurmTesting/ingest-nts-rheinKobAll.R &> tests/slurmTesting/ingest-nts-rheinKobAll.log &

library(ntsportal)
library(logger)
library(glue)
library(testthat)
dirTestResults <- "tests/slurmTesting/testResults-nts-rheinKobAll"
# After completion
# Remember to delete indices after ingest
logger::log_info("started")
startTime <- Sys.time()
newTableId <- ingestFeatureRecords(dirTestResults)
logger::log_info("ended")
endTime <- Sys.time()

index <- newTableId[[1]]$ntsp26.1_feature_bfg

message("Time needed to ingest: ", round(difftime(endTime, startTime, units = "mins")), " min")

message("Index name: ", index)
