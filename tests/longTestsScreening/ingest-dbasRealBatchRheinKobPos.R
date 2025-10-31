
library(logger)
library(ntsportal)
connectNtsportal()
log_info("started")
ingestFeatureRecords("tests/longTestsScreening/testResults", "ingest-feature")
log_info("ended")
