library(ntsportal)
library(glue)
###############
dirTestResults <- "tests/slurmTesting/testResults-nts-rheinKobAll"
msrawfileIndexName <- "ntsp26.1_msrawfiles"
userEmail <- Sys.getenv("USER_EMAIL")
##############

unlink(dirTestResults, recursive = T)
dir.create(dirTestResults)

dirs <- "/beegfs/nts/ntsportal/msrawfiles/rhein_koblenz_l/water/tof/p1d"

screeningSelectedBatchesSlurm(msrawfileIndexName, dirs, dirTestResults, userEmail, screeningType = "nts")

stopifnot(length(list.files(dirTestResults)) == 3)

system(glue("bash -c 'source /etc/profile.d/modules.sh && module load R/4.6.1 && sbatch {dirTestResults}/arrayScreening.sbatch'"))
