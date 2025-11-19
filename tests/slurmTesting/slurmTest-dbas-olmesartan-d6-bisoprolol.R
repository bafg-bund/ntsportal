

# Variables to be changed #################
userEmail <- "jewell@bafg.de"
dirTestResults <- "tests/slurmTesting/testResults-dbas-olmesartan-d6-bisoprolol"
msrawfileIndexName <- "ntsp25.3_msrawfiles_unit_tests"
###########################################

library(ntsportal)
file.remove(list.files(dirTestResults, f = T))
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6-bisoprolol/"
)

screeningSelectedBatchesSlurm(
  msrawfileIndex = msrawfileIndexName, 
  batchDirs = dirs, 
  saveDirectory = dirTestResults, 
  email = userEmail
)

stopifnot(length(list.files(dirTestResults)) == 3)

# User must manually submit job on SLURM terminal

# Once processing complete:
indexNames <- ingestFeatureRecords(dirTestResults)

# Cleanup
dbComm <- getDbComm()
deleteTable(dbComm, indexNames[[1]]$ntsp25.2_dbas_unit_tests)
file.remove(list.files(dirTestResults, f = T))
