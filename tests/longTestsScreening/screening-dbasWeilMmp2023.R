

# nohup Rscript screening-dbasWeilMmp2023.R &> dbasWeilMmp2023.log &
library(ntsportal)
tempSaveDir <- "testResults-dbasWeilMmp2023"
unlink(tempSaveDir, recursive = T)
dir.create(tempSaveDir)
index <- "ntsp25.2_msrawfiles"
batchDirectory <- "/beegfs/nts/ntsportal/msrawfiles/spm_upb/mmp/20250827_mmp_Weil_2023/"
                   
connectNtsportal()
startTime <- Sys.time()
screeningSelectedBatches(index, batchDirectory, tempSaveDir)
endTime <- Sys.time()
message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "mins")), " min")
