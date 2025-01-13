
library(ntsportal)

connectNtsportal()
file.remove(list.files("tests/slurmTesting/testResults", f = T))

index <- "ntsp_msrawfiles_unit_tests"
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6-bisoprolol/"
)
dbaScreeningSelectedBatchesSlurm(index, dirs, "tests/slurmTesting/testResults", "jewell@bafg.de")

stopifnot(length(list.files("tests/slurmTesting/testResults")) == 3)

# User must manually submit job

ingestJson("tests/slurmTesting/testResults")

# Cleanup
elastic::index_delete(escon, "ntsp_index_dbas_v250113135033_unit_tests")
