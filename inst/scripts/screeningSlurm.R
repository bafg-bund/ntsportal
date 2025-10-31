library(ntsportal)
connectNtsportal()

invocationDir <-  stringr::str_match(paste(commandArgs(), collapse = " "), "file=(.*)/screeningSlurm.R")[,2]
screeningType <- commandArgs(T)[1]
pathRecordsInBatches <- commandArgs(T)[2]
arrayTaskId <- as.numeric(commandArgs(T)[3])

logger::log_info("Processing SLURM_ARRAY_TASK_ID {arrayTaskId} in {pathRecordsInBatches}")

recordsInBatches <- readRDS(pathRecordsInBatches)

fileMade <- switch(
  screeningType, 
  dbas = dbaScreeningOneBatch(msrawfileRecords = recordsInBatches[[arrayTaskId]], saveDirectory = invocationDir),
  nts = ntScreeningOneBatch(msrawfileRecords = recordsInBatches[[arrayTaskId]], saveDirectory = invocationDir)
)

logger::log_info("Completed script with SLURM_ARRAY_TASK_ID {arrayTaskId} with result: {fileMade}")
