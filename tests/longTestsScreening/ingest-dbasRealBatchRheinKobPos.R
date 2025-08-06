
library(logger)
library(ntsportal)
connectNtsportal()
log_info("started")
ingest("tests/longTestsScreening/testResults", "ingest-feature")
log_info("ended")
