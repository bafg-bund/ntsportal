library(ntsportal)
saveDir <- "tests/slurmTesting/testResults-nts-rheinKobPos"
index <- "ntsp25.3_msrawfiles"
dirs <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
screeningSelectedBatchesSlurm(index, dirs, saveDir, "jewell@bafg.de", screeningType = "nts")

ingestFeatureRecords(saveDir)
