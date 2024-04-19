

# Functions for processing msrawfiles entries for nts

# temp #################
source("~/connect-ntsp.R")
rfindex <- "ntsp_index_msrawfiles_unit_tests"
tempsavedir <- "/scratch/nts/tmp"
coresBatch <- 1
res1 <- elastic::Search(
  escon, rfindex, size = 100, 
  body = list(
  
    sort = list(
      list(
        start = "asc"
      )
    )
  ), source = F)
esids <- sapply(res1$hits$hits, function(x) x[["_id"]])
# temp end ################
proc_batch_nts <- function(escon, rfindex, esids, tempsavedir, ingestpth, configfile, 
                            coresBatch, noIngest = FALSE) {
  # Define internal functions ####
  get_field2 <- get_field_builder(escon = escon, index = rfindex)
  
  proc_esid_pp <- function(escon, rfindex, esid) {  # esid <- esids[1]
    get_fieldx <- get_field_builder(escon = escon, index = rfindex)
    dataPath <- get_fieldx(esid, "path", justone = T)
    rawFile <- xcms::xcmsRaw(dataPath, includeMSn = TRUE)
    
    plTemp <- ntsworkflow::FindPeaks_BfG(
      rawFile, 
      get_fieldx(esid, "nts_mz_min", justone = T), 
      get_fieldx(esid, "nts_mz_max", justone = T), 
      get_fieldx(esid, "nts_mz_step", justone = T),
      get_fieldx(esid, "nts_rt_min", justone = T) * 60,
      get_fieldx(esid, "nts_rt_max", justone = T) * 60,
      get_fieldx(esid, "nts_sn", justone = T),
      get_fieldx(esid, "nts_int_threshold", justone = T),
      get_fieldx(esid, "nts_peak_noise_scans", justone = T),
      get_fieldx(esid, "nts_precursor_mz_tol", justone = T),
      get_fieldx(esid, "nts_peak_width_min", justone = T),
      get_fieldx(esid, "nts_peak_width_max", justone = T),
      get_fieldx(esid, "nts_max_num_peaks", justone = T)
    )
    
    plTemp2 <- ntsworkflow::componentization_BfG(
      plTemp, 
      daten = rawFile, 
      ppm = get_fieldx(esid, "nts_componentization_ppm", justone = T), 
      Grenzwert_RT = get_fieldx(esid, "nts_componentization_rt_tol", justone = T), 
      Grenzwert_FWHM_left = get_fieldx(esid, "nts_componentization_rt_tol_l", justone = T), 
      Grenzwert_FWHM_right = get_fieldx(esid, "nts_componentization_rt_tol_r", justone = T), 
      Summe_all = get_fieldx(esid, "nts_componentization_rt_tol_sum", justone = T),
      adjust_tolerance = get_fieldx(esid, "nts_componentization_dynamic_tolerance", justone = T)
    )
    
    plTemp2$RealPeak <- TRUE
    
    # Reduce size of rawFile
    rawFile@env$intensity <- NULL
    rawFile@env$mz <- NULL
    rawFile@env$profile <- NULL
    rawFile@env$msnIntensity <- NULL
    rawFile@env$msnMz <- NULL
    
    logger::log_info("Completed pp on file {dataPath}")
    
    list(pl = plTemp2, rf = rawFile)
  }
  
  # Start processing ####
  # Create sampleList
  sampleList <- data.frame(
    ID = seq_len(length(esids)),
    File = get_field2(esids, "path"),
    sampleType = ifelse(get_field2(esids, "blank"), "Blank", "Unknown"),
    optMzStep = get_field2(esids, "nts_mz_step"),
    RAM = FALSE,
    deleted = FALSE
  )
  
  log_info("Starting peak-picking and componentization")
  
  protopeaklist <- parallel::mclapply(
    esids, 
    proc_esid_pp, 
    escon = escon,
    rfindex = rfindex,
    mc.preschedule = FALSE,
    mc.cores = coresBatch
  )
  
  # 24-04-19 this is where I stopped
  
}