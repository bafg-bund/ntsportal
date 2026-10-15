

# nohup Rscript tests/longTestsScreening/screening-dbasRheinKobPos.R &> tests/longTestsScreening/dbasRheinKobPos.log &
library(ntsportal)
tempSaveDir <- "testResults-dbasRheinKobPos"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp26.1_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/rhein_koblenz_l/water/tof/p1d/2021/202105/pos/"
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir)
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
