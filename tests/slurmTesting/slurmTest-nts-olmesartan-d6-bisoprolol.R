

# Variables to be changed #################
userEmail <- "jewell@bafg.de"
dirTestResults <- "testResults-nts-olmesartan-d6-bisoprolol"
msrawfileIndexName <- "ntsp26.1_msrawfiles_unit_tests"
###########################################

library(ntsportal)

# Variables to be changed #################
userEmail <- Sys.getenv("USER_EMAIL")
dirTestResults <- "tests/slurmTesting/testResults-nts-olmesartan-d6-bisoprolol"
msrawfileIndexName <- "ntsp26.1_msrawfiles_unit_tests"
###########################################

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

Sys.sleep(1)

system("bash -c 'source /etc/profile.d/modules.sh && module load R/4.6.1 && sbatch tests/slurmTesting/testResults-nts-olmesartan-d6-bisoprolol/arrayScreening.sbatch'")

