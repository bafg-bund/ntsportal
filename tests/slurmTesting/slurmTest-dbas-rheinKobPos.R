library(ntsportal)

# Variables to be changed #################
userEmail <- Sys.getenv("USER_EMAIL")
dirTestResults <- "tests/slurmTesting/testResults-dbas-rheinKobPos"
msrawfileIndexName <- "ntsp26.1_msrawfiles"
###########################################

if (userEmail == "")
  stop("USER_EMAIL environment variable not correctly set")

dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/rhein_koblenz_l/water/tof/p1d/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/rhein_koblenz_l/water/tof/p1d/2021/202106/pos/"
)

unlink(dirTestResults, recursive = T)
dir.create(dirTestResults)
screeningSelectedBatchesSlurm(msrawfileIndexName, dirs, dirTestResults, userEmail)

stopifnot(length(list.files(dirTestResults)) == 3)

system("bash -c 'source /etc/profile.d/modules.sh && module load R/4.6.1 && sbatch tests/slurmTesting/testResults-dbas-rheinKobPos/arrayScreening.sbatch'")
