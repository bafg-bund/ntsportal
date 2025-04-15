
# first go to phil-hpc and start R in package dir, then call 

file.remove(list.files("tests/slurmTesting/testResults", f = T))





renv::deactivate()

# first update ntsworkflow and ntsportal in R user library

library(ntsportal)
connectNtsportal()
index <- "ntsp_msrawfiles"
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
dbaScreeningSelectedBatchesSlurm(index, dirs, tempDir, "jewell@bafg.de")
dbaScreeningSelectedBatchesSlurm(index, dirs, "tests/slurmTesting/testResults", "jewell@bafg.de")


stopifnot(any(grepl("\\.sbatch", list.files(tempDir))))

renv::activate()
# After finished

ingestJson("tests/slurmTesting/testResults")
