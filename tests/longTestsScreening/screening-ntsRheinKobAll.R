

# nohup Rscript screening-ntsRheinKobAll.R &> ntsRheinKobAll.log &
library(ntsportal)
tempSaveDir <- "testResultsRheinKobAll"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp25.2_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/koblenz/wasser"
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 15, screeningType = "nts")
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "hours")), " min")
