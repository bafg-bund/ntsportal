
library(ntsportal)

renv::deactivate()

# first update ntsworkflow and ntsportal in R user library

connectNtsportal()
file.remove(list.files("tests/slurmTesting/testResults", f = T))

index <- "ntsp25.1_msrawfiles_unit_tests"
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6-bisoprolol/"
)
dbaScreeningSelectedBatchesSlurm(index, dirs, "tests/slurmTesting/testResults", "jewell@bafg.de")

stopifnot(length(list.files("tests/slurmTesting/testResults")) == 3)

# User must manually submit job

indexNames <- ingestJson("tests/slurmTesting/testResults")

# Cleanup
elastic::index_delete(escon, indexNames[[1]]$ntsp25.1_dbas_unit_tests)

renv::activate()
