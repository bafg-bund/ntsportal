
# nohup Rscript screening-dbasRingversuchParallel.R &> dbasRingversuchParallel.log &
# nohup ntsp process -f -o "testResults-dbasRingversuchParallel" -p 4 -l "dbasRheinKobPosParallel.log" "/beegfs/nts/ntsportal/msrawfiles/bfg_lab/ringversuch/fants_2026" &
library(ntsportal)
tempSaveDir <- "testResults-dbasRingversuchParallel"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp26.1_msrawfiles"
batchDirectory <- c(
  "/beegfs/nts/ntsportal/msrawfiles/bfg_lab/ringversuch/fants_2026"
)

startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 4, screeningType = "dbas")
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
