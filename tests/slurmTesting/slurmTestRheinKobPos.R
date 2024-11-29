
library(ntsportal)

connectNtsportal()

file.remove(list.files("tests/slurmTesting/testResults", f = T))

index <- "ntsp_msrawfiles"
dirs <- c(
  "~/messdaten/koblenz/wasser/2021/202105/pos/",
  "~/messdaten/koblenz/wasser/2021/202106/pos/"
)
dbaScreeningSelectedBatchesSlurm(index, dirs, "tests/slurmTesting/testResults", "jewell@bafg.de")

stopifnot(length(list.files("tests/slurmTesting/testResults")) == 3)


# After finished

ingestJson("tests/slurmTesting/testResults")
