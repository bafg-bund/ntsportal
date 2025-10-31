library(ntsportal)
saveDir <- "tests/slurmTesting/testResultsNts"
unlink(saveDir, recursive = T)
dir.create(saveDir)
index <- "ntsp25.2_msrawfiles"
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
screeningSelectedBatchesSlurm(index, dirs, saveDir, "jewell@bafg.de", screeningType = "nts")

#ingestFeatureRecords("tests/slurmTesting/testResults", ingestPipeline = "ingest-feature")
