checkMsrawfiles()
dbComm <- newPythonComm()
version <- "25.1"

msrawfilesSetVersion(dbComm, "ntsp_msrawfiles", version)

# Variables to be changed #################
userEmail <- "jewell@bafg.de"
dirTestResults <- "tests/slurmTesting/testResults"
msrawfileIndexName <- "ntsp25.2_msrawfiles"
###########################################

renv::deactivate()  

library(ntsportal)
connectNtsportal()
file.remove(list.files(dirTestResults, f = T))


dbaScreeningUnprocessedBatchesSlurm(
  msrawfileIndex = msrawfileIndexName, 
  saveDirectory = dirTestResults, 
  email = userEmail
)
