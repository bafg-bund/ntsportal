library(ntsportal)
connectNtsportal()

invocationDir <-  stringr::str_match(paste(commandArgs(), collapse = " "), "file=(.*)/screeningSlurm.R")[,2]
pathRecordsInBatches <- commandArgs(T)[1]
arrayTaskId <- as.numeric(commandArgs(T)[2])

logger::log_info("Processing SLURM_ARRAY_TASK_ID {arrayTaskId} in {pathRecordsInBatches}")

recordsInBatches <- readRDS(pathRecordsInBatches)

rdsFilePath <- screeningOneBatch(msrawfilesBatch = recordsInBatches[[arrayTaskId]], saveDirectory = invocationDir)

logger::log_info("Completed script with SLURM_ARRAY_TASK_ID {arrayTaskId}, producing file {rdsFilePath}")
