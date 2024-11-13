
#' @export
screeningProcessAllStepsDbas <- function(RFINDEX, TEMPSAVE, 
                                         ROOTDIR_RF, CORES, CORESBATCH) 
  {

  library(ntsworkflow)
  library(logger)
  
  startTime <- lubridate::now()
  
  stopifnot(CORES == 1 || CORESBATCH == 1)
  
  check_integrity_msrawfiles(escon = escon, rfindex = RFINDEX, locationRf = ROOTDIR_RF)

  recordsInBatches <- getUnprocessedMsfiles(nameRawfilesIndex = RFINDEX, screeningType = "dbas")
  
  checkQualityBatchList(recordsInBatches)

  # Start processing ####
  log_info("Begin processing")
  
  numPeaksBatch <- parallel::mclapply(
    allFlsIds, 
    proc_batch, 
    escon = escon,
    rfindex = RFINDEX,
    tempsavedir = TEMPSAVE, 
    configfile = CONFG,
    coresBatch = CORESBATCH,
    mc.cores = CORES,
    mc.preschedule = FALSE
  )
  
  numPeaksBatch <- numPeaksBatch[sapply(numPeaksBatch, is.numeric)]
  numPeaksBatch <- as.numeric(numPeaksBatch)
  
  log_info("Completed all batches")
  log_info("Average peaks found per batch: {mean(numPeaksBatch)}")
  
  # Add analysis index ####
  if (any(grepl("_upb", allFlsIndex)))
    system2("Rscript", ADDANALYSIS)
  
  endTime <- lubridate::now()
  hrs <- round(as.numeric(endTime - startTime, units = "hours"))
  
  
}