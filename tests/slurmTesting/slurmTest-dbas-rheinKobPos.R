
# Variables to be changed #################
userEmail <- Sys.getenv("USER_EMAIL")
dirTestResults <- "tests/slurmTesting/testResults-dbas-rheinKobPos"
msrawfileIndexName <- "ntsp25.3_msrawfiles"
###########################################
library(ntsportal)

if (userEmail == "")
  stop("USER_EMAIL environment variable not correctly set")

dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)

screeningSelectedBatchesSlurm(msrawfileIndexName, dirs, dirTestResults, userEmail)

# After completion
# Remember to delete indices after ingest
logger::log_info("started")
ingestFeatureRecords(dirTestResults)
logger::log_info("ended")
