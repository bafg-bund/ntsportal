
# Variables to be changed #################
userEmail <- "jewell@bafg.de"
dirTestResults <- "tests/slurmTesting/testResults-dbas-rheinKobPos"
msrawfileIndexName <- "ntsp25.3_msrawfiles"
###########################################
library(logger)
library(ntsportal)
index <- "ntsp25.3_msrawfiles"
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
screeningSelectedBatchesSlurm(index, dirs, dirTestResults, userEmail)


# after completion
log_info("started")
ingestFeatureRecords(dirTestResults)
log_info("ended")