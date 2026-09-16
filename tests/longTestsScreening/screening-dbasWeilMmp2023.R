

# nohup Rscript screening-dbasWeilMmp2023.R &> dbasWeilMmp2023.log &
library(ntsportal)
tempSaveDir <- "testResults-dbasWeilMmp2023"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp26.1_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/rhein_weil_r/spm/p1m/2023"                   

startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir)
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
