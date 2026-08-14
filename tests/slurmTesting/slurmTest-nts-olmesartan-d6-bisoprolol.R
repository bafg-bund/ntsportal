

# Variables to be changed #################
userEmail <- "jewell@bafg.de"
dirTestResults <- "testResults-nts-olmesartan-d6-bisoprolol"
msrawfileIndexName <- "ntsp26.1_msrawfiles_unit_tests"
###########################################

library(ntsportal)
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6-bisoprolol/"
)
unlink(dirTestResults, recursive = T)
dir.create(dirTestResults)

screeningSelectedBatchesSlurm(
  msrawfileIndex = msrawfileIndexName, 
  batchDirs = dirs, 
  saveDirectory = dirTestResults, 
  email = userEmail,
  screeningType = "nts"
)

stopifnot(length(list.files(dirTestResults)) == 3)

# User must manually submit job on SLURM terminal

# Once processing complete:
indexNames <- ingestFeatureRecords(dirTestResults)

# Cleanup
dbComm <- getDbComm()
deleteTable(dbComm, indexNames[[1]]$ntsp25.2_dbas_unit_tests)
file.remove(list.files(dirTestResults, full.names = TRUE))
