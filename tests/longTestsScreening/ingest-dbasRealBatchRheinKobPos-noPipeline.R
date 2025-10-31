
library(logger)
library(ntsportal)
connectNtsportal()
log_info("started")
ingestFeatureRecords("tests/longTestsScreening/testResults/")
log_info("ended")
