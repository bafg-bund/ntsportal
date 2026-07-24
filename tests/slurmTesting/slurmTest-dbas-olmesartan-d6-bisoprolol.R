

# Variables to be changed #################
userEmail <- Sys.getenv("USER_EMAIL")
dirTestResults <- "testResults-dbas-olmesartan-d6-bisoprolol"
msrawfileIndexName <- "ntsp26.1_msrawfiles_unit_tests"
###########################################

library(ntsportal)
unlink(dirTestResults, recursive = T)
dir.create(dirTestResults)
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


