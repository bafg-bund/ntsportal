
#' @export
dbaScreeningNewBatches <- function(msrawfileIndex, saveDirectory) {
  recordsInBatches <- getUnprocessedMsrawfileRecords(msrawfileIndex, "dbas")
  dbaScreeningBatches(recordsInBatches, saveDirectory)
}

#' @export
dbaScreeningSelectedBatches <- function(msrawfileIndex, batchDirs, saveDirectory) {
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  dbaScreeningBatches(recordsInBatches, saveDirectory)
}

#' @export
dbaScreeningSelectedBatchesSlurm <- function(msrawfileIndex, batchDirs, saveDirectory) {
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  saveFilesSlurm(recordsInBatches, saveDirectory)
  slurmJobFile <- file.path(saveDirectory, "dbaScreeningSlurmArrayJob.sbatch")
  modifySlurmJobFile(slurmJobFile, length(recordsInBatches))

  submitCommand <- glue::glue("sbatch {slurmJobFile}")
  if (Sys.info()["nodename"] == "phil-hpc.bafg.de")  # slurm jobs can only be submitted form here
    system(submitCommand) else submitCommand
}

#' @export
dbaScreeningOneBatch <- function(msrawfileRecords, saveDirectory) {
  resultBatch <- scanBatchDbas(msrawfileRecords)
  featureRecordsBatch <- convertToRecord(resultBatch, msrawfileRecords)
  saveRecord(featureRecordsBatch, saveDirectory)
}



saveFilesSlurm <- function(recordsInBatches, saveDirectory) {
  saveRDS(recordsInBatches, file.path(saveDirectory, "recordsInBatches.RDS"))
  file.copy(fs::path_package("ntsportal", "scripts", "dbaScreeningSlurmArrayJob.sbatch"), saveDirectory)
  file.copy(fs::path_package("ntsportal", "scripts", "dbaScreeningSlurm.R"), saveDirectory)
}

modifySlurmJobFile <- function(jobFile, numberOfBatches) {
  system(glue::glue("sed -i '13s/RVarNumBatches/{numberOfBatches}/' {jobFile}"))
}


dbaScreeningBatches <- function(msrawfileRecordsInBatches, saveDirectory) {
  checkQualityBatchList(msrawfileRecordsInBatches)
  purrr::map_chr(msrawfileRecordsInBatches, dbaScreeningOneBatch, saveDirectory = saveDirectory)
}

