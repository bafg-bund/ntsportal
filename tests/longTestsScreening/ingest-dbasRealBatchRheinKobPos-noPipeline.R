
library(logger)
library(ntsportal)
connectNtsportal()
log_info("started")
ingest("tests/longTestsScreening/testResults/")
log_info("ended")
