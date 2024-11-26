
library(ntsportal)

connectNtsportal()

index <- "ntsp_index_msrawfiles_unit_tests"
dirs <- c(
  "~/ntsgz/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol/"
)
dbaScreeningSelectedBatchesSlurm(index, dirs, "testResults")
