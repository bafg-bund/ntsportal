# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

#' DBAS process all measurement files which have not yet been processed
#' 
#' @description
#' To determine which files have not yet been processed, the function
#' collects all directories in msrawfiles and looks in the feature indices
#' to see which are missing.
#' 
#' @param msrawfileIndex Name of index where processing information is stored
#' @param saveDirectory Location where SLURM job files should be saved
#' @param numParallel For local parallel processing via future
#'
#' @export
dbaScreeningNewBatches <- function(msrawfileIndex, saveDirectory, numParallel = 1) {
  recordsInBatches <- getUnprocessedMsrawfileRecords(msrawfileIndex, "dbas")
  dbaScreeningBatches(msrawfileRecordsInBatches, saveDirectory, numParallel)
}

#' DBAS process selected measurement files via batch directory
#' 
#' @description
#' Choose which batches to process by selecting the directory where the measurement files are stored. 
#' 
#' @param msrawfileIndex Name of index where processing information is stored
#' @param batchDirs Directory of measurement files (recursive, multiple permitted)
#' @param saveDirectory Location where SLURM job files should be saved
#' @param numParallel For local parallel processing via future
#'
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


#' Process one batch of records
#' 
#' @description
#' A function to be used only by the workload manager SLURM.
#' 
#' @export
dbaScreeningOneBatch <- function(msrawfileRecords, saveDirectory) {
  resultBatch <- scanBatchDbas(msrawfileRecords)
  featureRecordsBatch <- convertToRecord(resultBatch, msrawfileRecords)
  saveRecord(featureRecordsBatch, saveDirectory)
}

#' DBAS process batches by directory using the workload manager
#' 
#' @description
#' Choose which batches to process by selecting the directory where the measurement files are stored. 
#' The function will create necessary files for submission of the job to the workflow manager SLURM.
#' 
#' @param msrawfileIndex Name of index where processing information is stored
#' @param batchDirs Directory of measurement files (recursive, multiple permitted)
#' @param saveDirectory Location where SLURM job files should be saved
#' @param email Address for SLURM notifications
#'
#' @details
#' Three files are created, the records of all files to process, organized into batches (.RDS); the R script which
#' SLURM needs to run, and the SLURM job file. The command needed to start the process is given as a message (you
#' may need to switch to a SLURM-enabled server).
#'
#' @export
dbaScreeningSelectedBatchesSlurm <- function(msrawfileIndex, batchDirs, saveDirectory, email) {
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs)
  saveFilesSlurm(recordsInBatches, saveDirectory)
  slurmJobFile <- file.path(saveDirectory, "arrayDbaScreening.sbatch")
  
  addInfoToJob(slurmJobFile, saveDirectory, email, length(recordsInBatches))
  
  submitCommand <- glue::glue("sbatch {slurmJobFile}")
  message("Files are prepared, run \n$ ", submitCommand, "\non a Slurm-enabled terminal")
}

#' @export
dbaScreeningUnprocessedBatchesSlurm <- function(msrawfileIndex, saveDirectory, email) {
  recordsInBatches <- getUnprocessedMsrawfileBatches(msrawfileIndex, screeningType = "dbas")
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


