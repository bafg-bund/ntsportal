# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

#' @export
dbaScreeningNewBatches <- function(msrawfileIndex, saveDirectory, numParallel = 1) {
  recordsInBatches <- getUnprocessedMsrawfileRecords(msrawfileIndex, "dbas")
  dbaScreeningBatches(msrawfileRecordsInBatches, saveDirectory, numParallel)
}

#' @export
dbaScreeningSelectedBatches <- function(msrawfileIndex, batchDirs, saveDirectory, numParallel = 1) {
  msrawfileRecordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  dbaScreeningBatches(msrawfileRecordsInBatches, saveDirectory, numParallel)
}

dbaScreeningBatches <- function(msrawfileRecordsInBatches, saveDirectory, numParallel) {
  checkQualityBatchList(msrawfileRecordsInBatches)
  if (numParallel == 1) {
    dbaScreeningSerial(msrawfileRecordsInBatches, saveDirectory)
  } else {
    dbaScreeningParallel(msrawfileRecordsInBatches, saveDirectory)
  }
}

dbaScreeningSerial <- function(msrawfileRecordsInBatches, saveDirectory) {
  purrr::map_chr(msrawfileRecordsInBatches, dbaScreeningOneBatch, saveDirectory = saveDirectory)
}

dbaScreeningParallel <- function(msrawfileRecordsInBatches, saveDirectory, numParallel = 6) {
  if (rstudioapi::isAvailable()) {
    future::plan(multisession, workers = numParallel)
  } else {
    future::plan(multicore, workers = numParallel)
  }
  filePaths <- furrr::future_map(msrawfileRecordsInBatches, dbaScreeningOneBatch, saveDirectory = saveDirectory, .options = furrr::furrr_options(seed = NULL))
  plan(sequential)
  as.character(filePaths)
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


