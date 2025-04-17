

# Variables to be changed #################
userEmail <- "jewell@bafg.de"
dirTestResults <- "tests/slurmTesting/testResults"
msrawfileIndexName <- "ntsp25.1_msrawfiles_unit_tests"
###########################################

# renv does not work with the HPC, unknown why (maybe due to different Rocky Linux versions). The standard R user
# library ~/R/4.4.3 and /usr/lib64/R/library need to be tested and used for SLURM processing. Therefore deactivate renv
# and reactivate once the test is complete.
renv::deactivate()  

# Update ntsworkflow and ntsportal in R user library ~/R/4.4.3 
# (navigate to projects, pull and install using RStudio)

library(ntsportal)
connectNtsportal()
file.remove(list.files(dirTestResults, f = T))

dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6-bisoprolol/"
)

dbaScreeningSelectedBatchesSlurm(
  msrawfileIndex = msrawfileIndexName, 
  batchDirs = dirs, 
  saveDirectory = dirTestResults, 
  email = userEmail
)

stopifnot(length(list.files(dirTestResults)) == 3)

# User must manually submit job on phil-hpc (terminal)

indexNames <- ingestJson(dirTestResults)

# Cleanup
elastic::index_delete(escon, indexNames[[1]]$ntsp25.1_dbas_unit_tests)
renv::activate()
