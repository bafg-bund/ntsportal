
library(ntsportal)
library(logger)

recordsInBatchesPath <- commandArgs(T)[1]
batchId <- as.numeric(commandArgs(T)[2])

log_info("Processing batch {batchInd} in {batchPth}")

recordsInBatches <- readRDS(recordsInBatchesPath)

connectNtsportal()

fileMade <- dbaScreeningOneBatch(msrawfileRecords = recordsInBatches[[batchId]], saveDirectory = getwd())

log_info("completed script with result: {fileMade}")
