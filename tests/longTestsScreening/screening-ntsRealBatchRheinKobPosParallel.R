

# nohup Rscript screening-ntsRealBatchRheinKobPosParallel.R &> nts_processing.log &
library(ntsportal)
connectNtsportal()
tempSaveDir <- "testResultsParallel"
if (!dir.exists(tempSaveDir))
  dir.create(tempSaveDir)
index <- "ntsp25.2_msrawfiles"
batchDirectory <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2, screeningType = "nts")
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
