
# Variables to be changed #################
userEmail <- Sys.getenv("USER_EMAIL")
dirTestResults <- "testResults-nts-rheinKobPos"
msrawfileIndexName <- "ntsp26.1_msrawfiles"
###########################################

library(ntsportal)
if (userEmail == "")
  stop("USER_EMAIL environment variable not correctly set")
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
unlink(dirTestResults, recursive = T)
dir.create(dirTestResults)

screeningSelectedBatchesSlurm(msrawfileIndexName, dirs, dirTestResults, userEmail, screeningType = "nts")

# After completion
# Remember to delete indices after ingest
logger::log_info("started")
ingestFeatureRecords(dirTestResults)
logger::log_info("ended")

