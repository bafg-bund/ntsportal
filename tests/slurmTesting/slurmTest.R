
library(ntsportal)

connectNtsportal()
file.remove(list.files("tests/slurmTesting/testResults", f = T))

index <- "ntsp_index_msrawfiles_unit_tests"
dirs <- c(
  "~/ntsgz/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol/"
)
dbaScreeningSelectedBatchesSlurm(index, dirs, "tests/slurmTesting/testResults", "thron@bafg.de")

stopifnot(length(list.files("tests/slurmTesting/testResults")) == 3)


ingestJson("tests/slurmTesting/testResults")
