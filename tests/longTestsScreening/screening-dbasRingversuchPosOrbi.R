
# nohup Rscript screening-dbasRingversuchPosOrbi.R &> dbasRingversuchPosOrbi.log &
library(ntsportal)
tempSaveDir <- "testResults-dbasRingversuchPosOrbi"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp26.1_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/bfg_lab/ringversuch/fants_2026/orbi/pos"
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir)
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
