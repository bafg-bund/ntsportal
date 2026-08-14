library(ntsportal)

dirTestResults <- "testResults-dbas-olmesartan-d6-bisoprolol"
# User must manually submit job on SLURM terminal

# Once processing complete:
indexNames <- ingestFeatureRecords(dirTestResults)

# Cleanup
dbComm <- getDbComm()
deleteTable(dbComm, indexNames[[1]]$ntsp25.3_feature_unit_tests)
file.remove(list.files(dirTestResults, full.names = TRUE))

