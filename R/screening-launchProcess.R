
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
dbaScreeningOneBatch <- function(msrawfileRecords, saveDirectory) {
  resultBatch <- scanBatchDbas(msrawfileRecords)
  featureRecordsBatch <- convertToRecord(resultBatch, msrawfileRecords)
  saveRecord(featureRecordsBatch, saveDirectory)
}

#' @export
dbaScreeningSelectedBatchesSlurm <- function(msrawfileIndex, batchDirs, saveDirectory, email) {
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  saveFilesSlurm(recordsInBatches, saveDirectory)
  slurmJobFile <- file.path(saveDirectory, "arrayDbaScreening.sbatch")
  
  addInfoToJob(slurmJobFile, saveDirectory, email, length(recordsInBatches))
  
  submitCommand <- glue::glue("sbatch {slurmJobFile}")
  message("Files are prepared, run \n$ ", submitCommand, "\non a Slurm-enabled terminal")
  invisible(submitCommand)
}

saveFilesSlurm <- function(recordsInBatches, saveDirectory) {
  saveRDS(recordsInBatches, file.path(saveDirectory, "recordsInBatches.RDS"))
  file.copy(fs::path_package("ntsportal", "scripts", "arrayDbaScreening.sbatch"), saveDirectory)
  file.copy(fs::path_package("ntsportal", "scripts", "dbaScreeningSlurm.R"), saveDirectory)
}

addInfoToJob <- function(jobFile, saveDirectory, email, numberOfBatches) {
  addTextToJob(jobFile, "RVarDir", saveDirectory)
  addTextToJob(jobFile, "RVarEmail", email)
  addTextToJob(jobFile, "RVarNumBatches", numberOfBatches)
}

addTextToJob <- function(jobFile, variableName, value) {
  system(glue::glue("sed -i 's|{variableName}|{value}|g' {jobFile}"))
}

dbaScreeningBatches <- function(msrawfileRecordsInBatches, saveDirectory) {
  checkQualityBatchList(msrawfileRecordsInBatches)
  purrr::map_chr(msrawfileRecordsInBatches, dbaScreeningOneBatch, saveDirectory = saveDirectory)
}
