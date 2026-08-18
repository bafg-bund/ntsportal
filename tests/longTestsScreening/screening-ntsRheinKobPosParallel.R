

# nohup Rscript screening-ntsRheinKobPosParallel.R &> ntsRheinKobPosParallel.log &
library(ntsportal)
connectNtsportal()
tempSaveDir <- "testResults-ntsRheinKobPosParallel"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp26.1_msrawfiles"
batchDirectory <- c(
  "/beegfs/nts/ntsportal/msrawfiles/rhein_koblenz_l/water/tof/p1d/2021/202105/pos/",
  "/beegfs/nts/ntsportal/msrawfiles/rhein_koblenz_l/water/tof/p1d/2021/202106/pos/"
)
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2, screeningType = "nts")
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
