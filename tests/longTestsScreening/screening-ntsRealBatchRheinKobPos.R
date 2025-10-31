# nohup Rscript screening-ntsRealBatchRheinKobPos.R &> nts_processing.log &
library(ntsportal)
tempSaveDir <- "testResults"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp25.2_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/"
connectNtsportal()
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir, screeningType = "nts")
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
