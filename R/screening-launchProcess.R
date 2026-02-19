

#' Process measurement files in selected batch directories
#' @description Choose which batches to process by selecting the directory where the measurement files are stored. 
#' @param msrawfileIndex Name of index where processing information is stored
#' @param saveDirectory Location where files will be saved. Directory will be created. Warning: files may be 
#' overwritten if the directory already exists 
#' @param numParallel For local parallel processing via future
#' @param screeningType either 'dbas' or 'nts' for library screening and non-target-screening, respectively
#' @param batchDirs Directory of measurement files (recursive, multiple permitted)
#' @export
screeningSelectedBatches <- function(msrawfileIndex, batchDirs, saveDirectory, numParallel = 1, screeningType = "dbas") {
  checkScreeningType(screeningType)
  createSaveDir(saveDirectory)
  msrawfileRecordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs, screeningType)
  screeningBatches(msrawfileRecordsInBatches, saveDirectory, numParallel)
}

screeningBatches <- function(msrawfileRecordsInBatches, saveDirectory, numParallel) {
  checkQualityBatchList(msrawfileRecordsInBatches)
  if (numParallel == 1) {
    screeningSerial(msrawfileRecordsInBatches, saveDirectory, screeningType)
  } else {
    screeningParallel(msrawfileRecordsInBatches, saveDirectory, numParallel = numParallel, screeningType)
  }
}

screeningSerial <- function(msrawfileRecordsInBatches, saveDirectory, screeningType) {
  purrr::map_chr(
    msrawfileRecordsInBatches, 
    screeningOneBatch, 
    saveDirectory = saveDirectory
  )
}

screeningParallel <- function(msrawfileRecordsInBatches, saveDirectory, numParallel = 6, screeningType) {
  if (rstudioapi::isAvailable()) {
    future::plan(multisession, workers = numParallel)
  } else {
    future::plan(multicore, workers = numParallel)
  }
  filePaths <- furrr::future_map(
    msrawfileRecordsInBatches,
    screeningOneBatch, 
    saveDirectory = saveDirectory, 
    .options = furrr::furrr_options(seed = NULL)
  )
  plan(sequential)
  as.character(filePaths)
}

#' Process one batch of measurement files
#' @description Perform file scanning, converts the result to the NTSPortal `featureRecord` format and saves it
#' to RDS.  
#' @param msrawfilesBatch object inheriting `msrawfilesBatch`
#' @export
screeningOneBatch <- function(msrawfilesBatch, saveDirectory) {
  resultBatch <- scanBatch(msrawfilesBatch)
  featureRecordsBatch <- convertToRecord(resultBatch, msrawfilesBatch)
  saveRecord(featureRecordsBatch, saveDirectory)
}

#' Process batches using SLURM by selecting directories
#' @description Choose which batches to process by selecting the directory where the measurement files are stored. 
#' It will create the necessary files for submission of the job to the workflow manager SLURM in `saveDirectory`.
#' @inheritParams screeningSelectedBatches
#' @param email Email recipient for SLURM notifications
#' @details
#' Three files are created, the records of all files to process, organized into batches (.RDS); the R script which
#' SLURM needs to run, and the SLURM job file (`.sbatch`). The command needed to start the process is given as a 
#' message (you may need to switch to a SLURM-enabled server).
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
#' screeningSelectedBatchesSlurm(
#'   msrawfileIndex = msrawfileIndexName,
#'   batchDirs = "/root/dir/all/msrawfiles",
#'   saveDirectory = dirResults,
#'   email = userEmail
#' )
#' }
screeningSelectedBatchesSlurm <- function(msrawfileIndex, batchDirs, saveDirectory, email, screeningType = "dbas") {
  checkScreeningType(screeningType)
  createSaveDir(saveDirectory)
  recordsInBatches <- getSelectedMsrawfileBatches(msrawfileIndex, batchDirs, screeningType)
  checkQualityBatchList(recordsInBatches)
  saveFilesSlurm(recordsInBatches, saveDirectory)
  slurmJobFile <- file.path(saveDirectory, "arrayScreening.sbatch")
  
  addInfoToJob(slurmJobFile, saveDirectory, email, length(recordsInBatches))
  
  submitCommand <- glue::glue("sbatch {slurmJobFile}")
  message("Files are prepared, run \n$ ", submitCommand, "\non a SLURM-enabled terminal")
}

createSaveDir <- function(saveDirectory) {
  if (!dir.exists(saveDirectory)) {
    dir.create(saveDirectory)
  } else {
    message(saveDirectory, " already exists, files will be overwritten")
  }
}

checkScreeningType <- function(screeningType) {
  screeningTypeOptions <- c("dbas", "nts")
  if (!screeningType %in% screeningTypeOptions)
    stop("screeningType must be one of ", paste(screeningTypeOptions, collapse = ", "))
}
saveFilesSlurm <- function(recordsInBatches, saveDirectory) {
  saveRDS(recordsInBatches, file.path(saveDirectory, "recordsInBatches.RDS"))
  file.copy(fs::path_package("ntsportal", "scripts", "arrayScreening.sbatch"), saveDirectory, overwrite = TRUE)
  file.copy(fs::path_package("ntsportal", "scripts", "screeningSlurm.R"), saveDirectory, overwrite = TRUE)
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
