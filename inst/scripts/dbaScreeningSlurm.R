
invocationDir <-  stringr::str_match(paste(commandArgs(), collapse = " "), "file=(.*)/dbaScreeningSlurm.R")[,2]
recordsInBatchesPath <- commandArgs(T)[1]
arrayTaskId <- as.numeric(commandArgs(T)[2])

logger::log_info("Processing SLURM_ARRAY_TASK_ID {arrayTaskId} in {batchPth}")

recordsInBatches <- readRDS(recordsInBatchesPath)

fileMade <- ntsportal::dbaScreeningOneBatch(msrawfileRecords = recordsInBatches[[batchId]], saveDirectory = invocationDir)

logger::log_info("Completed script with SLURM_ARRAY_TASK_ID {arrayTaskId} with result: {fileMade}")
