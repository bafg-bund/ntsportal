library(ntsportal)
saveDir <- "tests/slurmTesting/testResultsNtsRheinKobAll"
unlink(saveDir, recursive = T)
dir.create(saveDir)
index <- "ntsp25.2_msrawfiles"
dirs <- "/beegfs/nts/ntsportal/msrawfiles/koblenz"
screeningSelectedBatchesSlurm(index, dirs, saveDir, "jewell@bafg.de", screeningType = "nts")

#ingestFeatureRecords("tests/slurmTesting/testResults", ingestPipeline = "ingest-feature")
