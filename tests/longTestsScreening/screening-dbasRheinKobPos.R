

# nohup Rscript screening-dbasRheinKobPos.R &> dbasRheinKobPos.log &
library(ntsportal)
tempSaveDir <- "testResults-dbasRheinKobPos"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp25.3_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser/2021/202105/pos/"
connectNtsportal()
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir)
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
