
#' Process all measurement files which have not yet been processed by DBAS
#' @description To determine which files have not yet been processed, the function
#' collects all directories in `msrawfiles` and looks in the `feature` indices
#' to see which are missing.
#' @param msrawfileIndex Name of index where processing information is stored
#' @param saveDirectory Location where results should be saved
#' @param numParallel For local parallel processing via future
#' @export
dbaScreeningNewBatches <- function(msrawfileIndex, saveDirectory, numParallel = 1) {
  recordsInBatches <- getUnprocessedMsrawfileRecords(msrawfileIndex, "dbas")
  dbaScreeningBatches(msrawfileRecordsInBatches, saveDirectory, numParallel)
}

#' DBAS process selected measurement files via batch directory
#' @description Choose which batches to process by selecting the directory where the measurement files are stored. 
#' @inheritParams dbaScreeningNewBatches
#' @param batchDirs Directory of measurement files (recursive, multiple permitted)
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
    dbaScreeningParallel(msrawfileRecordsInBatches, saveDirectory, numParallel = numParallel)
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
  filePaths <- furrr::future_map(
    msrawfileRecordsInBatches,
    dbaScreeningOneBatch, 
    saveDirectory = saveDirectory, 
    .options = furrr::furrr_options(seed = NULL)
  )
  plan(sequential)
  as.character(filePaths)
}

#' Process one batch of measurement files by DBAS
#' @description Perform file scanning (DBAS), convert the result to the NTSPortal `featureRecord` format and save it
#' to as a JSON.  
#' @inheritParams dbaScreeningNewBatches
#' @export
dbaScreeningOneBatch <- function(msrawfileRecords, saveDirectory) {
  resultBatch <- scanBatchDbas(msrawfileRecords)
  featureRecordsBatch <- convertToRecord(resultBatch, msrawfileRecords)
  saveRecord(featureRecordsBatch, saveDirectory)
}

#' Process batches (DBAS) using SLURM by selecting directories
#' @description Choose which batches to process by selecting the directory where the measurement files are stored. 
#' `dbaScreeningSelectedBatchesSlurm()` will create necessary files for submission of the job to the workflow manager SLURM.
#' @inheritParams dbaScreeningSelectedBatches
#' @param saveDirectory Location where SLURM job files should be saved
#' @param email Address for SLURM notifications
#'
#' @details
#' Three files are created, the records of all files to process, organized into batches (.RDS); the R script which
#' SLURM needs to run, and the SLURM job file (.sbatch). The command needed to start the process is given as a message (you
#' may need to switch to a SLURM-enabled server).
#' @export
#' @examples
#' \dontrun{
#' userEmail <- "example@bafg.de"
#' dirResults <- "processingResults"
#' msrawfileIndexName <- "ntsp25.2_msrawfiles"
#' library(ntsportal)
#' connectNtsportal()
#' file.remove(list.files(dirResults, f = T))
#'
#' dbaScreeningSelectedBatchesSlurm(
#'   msrawfileIndex = msrawfileIndexName,
#'   batchDirs = "/root/dir/all/msrawfiles",
#'   saveDirectory = dirResults,
#'   email = userEmail
#' )
#' }
#'
dbaScreeningSelectedBatchesSlurm <- function(msrawfileIndex, batchDirs, saveDirectory, email) {
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  checkQualityBatchList(recordsInBatches)
  saveFilesSlurm(recordsInBatches, saveDirectory)
  slurmJobFile <- file.path(saveDirectory, "arrayDbaScreening.sbatch")
  
  addInfoToJob(slurmJobFile, saveDirectory, email, length(recordsInBatches))
  
  submitCommand <- glue::glue("sbatch {slurmJobFile}")
  message("Files are prepared, run \n$ ", submitCommand, "\non a SLURM-enabled terminal")
}

#' Process new batches (DBAS) using SLURM
#' @description Only new batches are selected, see `dbaScreeningNewBatches()`. The function creates 
#' the necessary file to submit the processing job to a SLURM workload manager. See `dbaScreeningSelectedBatchesSlurm()`
#' @inheritParams dbaScreeningSelectedBatchesSlurm
#' @export
dbaScreeningUnprocessedBatchesSlurm <- function(msrawfileIndex, saveDirectory, email) {
  recordsInBatches <- getUnprocessedMsrawfileBatches(msrawfileIndex, screeningType = "dbas")
  checkQualityBatchList(recordsInBatches)
  saveFilesSlurm(recordsInBatches, saveDirectory)
  slurmJobFile <- file.path(saveDirectory, "arrayDbaScreening.sbatch")
  
  addInfoToJob(slurmJobFile, saveDirectory, email, length(recordsInBatches))
  
  submitCommand <- glue::glue("sbatch {slurmJobFile}")
  message("Files are prepared, run \n$ ", submitCommand, "\non a Slurm-enabled terminal")
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

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
