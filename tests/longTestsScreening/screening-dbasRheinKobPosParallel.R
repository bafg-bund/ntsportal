

# nohup Rscript screening-dbasRheinKobPosParallel.R &> dbasRheinKobPosParallel.log &
library(ntsportal)
connectNtsportal()
tempSaveDir <- "testResults-dbasRheinKobPos-parallel/"
if (!dir.exists(tempSaveDir))
  dir.create(tempSaveDir)
index <- "ntsp25.3_msrawfiles"
batchDirectory <- c(
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202106/pos/"
)

startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2, screeningType = "dbas")
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
