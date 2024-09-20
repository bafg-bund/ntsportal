# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.





# Functions for processing msrawfiles entries for nts

#' Process batches and save results all steps
#' 
#' @description
#' This function combines all the functions starting from the parameters saved 
#' in an msrawfiles_docs_list (from msrawfiles index) until the  
#' 
#'
#' @param batches ntsp msrawfiles as an msrawfiles_docs_list, as provided by elasticsearch API
#' @param saveDir where to save files
#' @param coresTotal number of cores to use
#' @param saveIntermed save intermediate results for bugfixing
#'
#' @return vector of paths to newly created json files
#' @export
#'
proc_batches_nts <- function(batches, saveDir, coresTotal = 1, saveIntermed = F) {
  
  # Proc each batch and save JSON file
  
  all_steps <- function(dl) {
    
    ranThrough <- FALSE
    batchDir <- dirname(dl[[1]][["_source"]][["path"]])
    batchDirSave <- gsub("/", "_", batchDir)
    batchDirSave <- gsub("\\.", "_", batchDirSave)
    log_info("Running batch {batchDir}")
    tryCatch({
      procoBatch <- proc_batch_nts(dl, coresBatch = 1)
      if (saveIntermed)
        saveRDS(procoBatch, file.path(saveDir, paste0("proco-", batchDirSave, ".RDS")))
      ntsplBatch <- make_ntspl(procoBatch, coresBatch = 1)
      if (saveIntermed)
        saveRDS(ntsplBatch, file.path(saveDir, paste0("ntspl-", batchDirSave, ".RDS")))
      # pth can be vector of multiple paths, if ntspl object was too large and
      # needed to be split
      pth <- save_ntspl(ntsplBatch, saveDir = saveDir)
      jsonPth <- normalizePath(pth)
      ranThrough <- TRUE
    },
    error = function(cnd) {
      log_error("Error in batch {batchDir}, message: {conditionMessage(cnd)}, call: {conditionCall(cnd)}")
    },
    finally = suppressWarnings(rm(procoBatch, ntsplBatch))
    )
    if (ranThrough) {
      for (p in pth)
        system2("gzip", p)
      log_info("Finished batch {batchDir}")
      return(data.frame(path_batch = batchDir, path_json = jsonPth, success = ranThrough))
    } else {
      return(data.frame(path_batch = batchDir, path_json = NA, success = ranThrough))
    }
    
  }
  
  if (coresTotal == 1) {
    results <- purrr::map(batches, all_steps)
  } else {
    stop("multicore not available at the moment")
    # if (rstudioapi::isAvailable()) {
    #   plan(multisession, workers = coresTotal)  
    # } else {
    #   plan(multicore, workers = coresTotal)
    # }
    # # You must set seed = NULL otherwise 'future' will give you warnings about
    # # reproducible random number generation...
    # results <- furrr::future_map(batches, all_steps, .options = furrr::furrr_options(seed = 123))
    # plan(sequential)
  }
  
  
  log_info("Ran through all batches")
  
  rdf <- do.call("rbind", results)
  
  if (sum(!rdf$success) > 0) {
    failedBatchDirs <- rdf$path_batch[!rdf$success]
    log_info("Failed batches: {paste(failedBatchDirs, collapse = ',\n')}")
  } else {
    log_info("All batches successful")
  }
  fn <- paste0("completed-batches-", format(Sys.time(), "%y%m%d_%H%M"), ".csv")
  rdf <- rdf[!is.na(rdf$path_json), ]
  rdf$json_path_gz <- paste0(rdf$path_json, ".gz")
  write.csv(rdf, file.path(saveDir, fn))
 
  rdf
}


#' Process batches with new files
#'
#' @param msrawfiles 
#' @param saveDir 
#' @param coresBatch 
#'
#' @return vector of paths to newly created json files
#' @export
#'
proc_nts_new <- function(msrawfiles, saveDir, coresTotal = 1) {
  
  # Get batches with unprocessed files
  batches <- get_unproc_batches(msrawfiles, "nts")
  
  proc_batches_nts(batches, saveDir, coresTotal)
}


#' Process a single file using a list of settings
#' 
#' The settings used is a document source of the msrawfiles index
#' 
#' @param dsrc document source
#'
#' @return a list with 3 elements, named pl (peaklist), rf (rawfile), 
#' and set (settings)
#'
proc_doc_pp <- function(dsrc) {  # esid <- esids[1]
  failed <- FALSE
  tryCatch(
    rawFile <- suppressMessages(xcms::xcmsRaw(dsrc$path, includeMSn = TRUE)),
    error = function(cnd) {
      log_warn("Error in opening file {dsrc$path}: {conditionMessage(cnd)}")
      failed <<- TRUE
    }
  )
  if (failed)
    return(NULL)
  
  tryCatch(
    plTemp <- ntsworkflow::FindPeaks_BfG(
      rawFile, 
      dsrc$nts_mz_min, 
      dsrc$nts_mz_max, 
      dsrc$nts_mz_step,
      dsrc$nts_rt_min * 60,
      dsrc$nts_rt_max * 60,
      dsrc$nts_sn,
      dsrc$nts_int_threshold,
      dsrc$nts_peak_noise_scans,
      dsrc$nts_precursor_mz_tol,
      dsrc$nts_peak_width_min,
      dsrc$nts_peak_width_max,
      dsrc$nts_max_num_peaks
    ),
    error = function(cnd) {
      log_warn("Error in peak-picking {dsrc$path}: {conditionMessage(cnd)}")
      failed <<- TRUE
    }
  )
  if (failed)
    return(NULL)
  if (nrow(plTemp) == 0)
    return(NULL)
  tryCatch(
    plTemp2 <- ntsworkflow::componentization_BfG(
      plTemp, 
      daten = rawFile, 
      ppm = dsrc$nts_componentization_ppm, 
      Grenzwert_RT = dsrc$nts_componentization_rt_tol, 
      Grenzwert_FWHM_left = dsrc$nts_componentization_rt_tol_l, 
      Grenzwert_FWHM_right = dsrc$nts_componentization_rt_tol_r, 
      Summe_all = dsrc$nts_componentization_rt_tol_sum,
      adjust_tolerance = dsrc$nts_componentization_dynamic_tolerance
    ),
    error = function(cnd) {
      log_warn("Error in componentization {dsrc$path}: {conditionMessage(cnd)}")
      failed <<- TRUE
    }
  )
  if (failed)
    return(NULL)
  
  plTemp2$RealPeak <- TRUE
  
  # Reduce size of rawFile
  rawFile@env$intensity <- NULL
  rawFile@env$mz <- NULL
  rawFile@env$profile <- NULL
  rawFile@env$msnIntensity <- NULL
  rawFile@env$msnMz <- NULL
  
  # Create PeakPickSettings (has no purpose other than allowing you to open RDS file in the app)
  
  settings <- list()
  settings$massrange <- c(
    dsrc$nts_mz_min, 
    dsrc$nts_mz_max
  )
  settings$mz_step <- dsrc$nts_mz_step
  settings$rtrange <- c(
    dsrc$nts_rt_min * 60, 
    dsrc$nts_rt_max * 60
  )
  settings$peakwidth <- c(
    dsrc$nts_peak_width_min,
    dsrc$nts_peak_width_max
  )
  
  settings$NoiseScans <- dsrc$nts_peak_noise_scans
  settings$sn <- dsrc$nts_sn
  settings$int_threshold <- dsrc$nts_int_threshold
  settings$precursormzTol <- dsrc$nts_precursor_mz_tol
  settings$ppm <- dsrc$nts_componentization_ppm
  settings$RT_Tol <- dsrc$nts_componentization_rt_tol
  settings$PPTableRow <- 1  # I changed this to 1, the whole thing makes no sense to me
  settings$orderType <- list(list(0,"asc"))  # what is the point?
  settings$TableLength <- 10  # what is the point?
  settings$displayStart <- 0  # what is the point?
  settings$maxNumPeaks <- dsrc$nts_max_num_peaks
  
  logger::log_info("Completed pp on file {basename(dsrc$path)}")
  
  list(pl = plTemp2, rf = rawFile, set = settings)
}







#' Process a batch of files using ntsworkflow
#' 
#' @description Will take a list of documents and process samples in them for 
#' peak-picking, alignment, blank correction, annotation
#'
#' @param docsList list of documents as returned by elasticSearch API (hits.hits array) 
#' @param coresBatch Number of cores to use in a signal batch
#' 
#' @details
#' # Filtering the alignment table 
#' ## Consecutive filter 
#' The order of the samples is important for the
#' consecutive filter, these must be sorted by start time prior to starting
#' this processing.
#' 
#' @return A proc_output list with 6 elements:  
#' @export
#'
proc_batch_nts <- function(docsList, coresBatch) {

  # Define variables ####
  
  # Extract just the source of the array
  docsSrc <- lapply(docsList, "[[", i = "_source")
 
  
  # Use all spec sources for annotate_grouped
  dbPath <- gf(docsSrc, "nts_spectral_library", character(1), justone = T)
  sdb <- con_sqlite(dbPath)
  specSource <- tbl(sdb, "experimentGroup") %>% 
    select(name) %>% distinct() %>% collect() %>% .$name
  DBI::dbDisconnect(sdb)
  # TODO change annotate_grouped so that the instruments and sources do not need
  # to be explicitly passed. Just have NULL or all which will include all of them.
  
  # Start processing ####
  # Create sampleList
  create_sampleList <- function(dl, ds) {
    data.frame(
      ID = seq_along(dl),
      File = gf(ds, "path", character(1)),
      sampleType = ifelse(gf(ds, "blank", logical(1)), "Blank", "Unknown"),
      optMzStep = gf(ds, "nts_mz_step", numeric(1)),
      RAM = FALSE,
      deleted = FALSE,
      rfindexEsid = vapply(dl, "[[", i = "_id", character(1))
    )
  }
  sampleList <- create_sampleList(docsList, docsSrc)
  batchName <- dirname(docsSrc[[1]]$path)
  
  log_info("Starting peak-picking and componentization on batch {batchName}")
  
  # Peak-picking ####
  # Multisession for RStudio, multicore for processing (multicore uses less memory)
  if (coresBatch == 1) {
    protopeaklist <- purrr::map(docsSrc, proc_doc_pp)
  } else {
    stop("we can't use furrr yet")
    # if (rstudioapi::isAvailable()) {
    #   plan(multisession, workers = coresBatch)  
    # } else {
    #   plan(multicore, workers = coresBatch)
    # }
    # protopeaklist <- furrr::future_map(docsSrc, proc_doc_pp, 
    #                                    .options = furrr::furrr_options(seed = NULL))
    # plan(sequential)
  }
  
  
  # Check results
  # Remove any NULL values from all variables
  if (all(sapply(protopeaklist, is.null)))
    stop("Peak picking unsuccessful for all samples in batch", dirname(docsSrc[[1]]$path))
  
  # Cull the results of duds
  if (any(sapply(protopeaklist, is.null))) {
    badFiles <- which(sapply(protopeaklist, is.null))
    log_warn("In batch {dirname(docsSrc[[1]]$path)} these files failed: {paste(sampleList[badFiles, 'File'], collapse = ',\n')}")
    protopeaklist <- protopeaklist[-badFiles]
    docsSrc <- docsSrc[-badFiles]
    docsList <- docsList[-badFiles]
    # Rebuild sampleList
    sampleList <- create_sampleList(docsList, docsSrc)
  }
  
  if (!all(vapply(protopeaklist, function(x) all(c("pl", "rf", "set") %in% names(x)), logical(1))))
    stop("Peak picking unsuccessful in batch", dirname(docsSrc[[1]]$path))
  
  peaklist <- lapply(protopeaklist, function(x) x[["pl"]])
  datenList <- lapply(protopeaklist, function(x) x[["rf"]])
  peakPickSettings <- lapply(protopeaklist, function(x) x[["set"]])
  
  # Add ids to all peaks
  peaklist <- ntsworkflow::new_peaklist(peaklist, datenList)
  # Reorder all peaklists by m/z
  peaklist <- lapply(peaklist, function(x) x[order(x$mz),])
  
  rm(protopeaklist)
  
  log_info("Peak-picking complete")
  
  # Alignment ####
  log_info("Starting alignment")
  grouped <- ntsworkflow::alignment_BfG_cpp(
    peaklist, 
    gf(docsSrc, "nts_alig_delta_mz", numeric(1), justone = T), 
    gf(docsSrc, "nts_alig_delta_rt", numeric(1), justone = T), 
    gf(docsSrc, "nts_alig_mz_tol_units", character(1), justone = T)
  )
  # Collect componentization information from samples to column "Gruppe"
  grouped <- ntsworkflow::summarize_groups(grouped)
  
  # Add alignment ID column
  oldnames <- colnames(grouped)
  grouped <- cbind(grouped, seq_len(nrow(grouped)))
  colnames(grouped) <- c(oldnames, "alignmentID")
  log_info("Completed alignment")
  
  aligRowsUncorrected <- nrow(grouped)
  
  # Second stage componentization ####
  #log_info("Second stage componentization skipped")
  # log_info("Second stage componentization on batch size {length(esids)}")
  # Update column "Gruppe" with new information
  # Commented out for now because it takes too long
  # grouped <- ntsworkflow::alig_componentisation(
  #   altable = grouped,
  #   rttols = gfield(esids, "nts_componen2_rt_tol", justone = T), 
  #   fracComponMatch = gfield(esids, "nts_componen2_frac_shape", justone = T), 
  #   mztol = gfield(esids, "nts_componen2_mz_tol", justone = T) / 1000, 
  #   pearsonCorr = gfield(esids, "nts_componen2_correlation", justone = T), 
  #   pol = gfield(esids, "pol", justone = T)
  # )
  # log_info("Completed second stage componentization")
  
  # Alignment filter ####  
  # Consecutive, triplicate injection or minimum features filter
  # You have created a new sampleList after culling the peaklist and before
  # alignment. Therefore the sampleList$ID column, the alignment table
  # column headers and the index of docsSrc should match at this stage
  
  # The filtering only applies to non-blanks ("unknowns" in ntsworkflow)
  # (expect for min_features, this applies to all samples)
  sc <- sampleList[sampleList$sampleType == "Unknown", "ID"]
  ftype <- gf(docsSrc[sc], "nts_alig_filter_type", character(1), justone = T)
  log_info("Alignment table filter of type {ftype}")
  switch(
    ftype,
    consecutive = {
      # For this to work the samples must be ordered by start (sample time).
      # This would be super hard to change at this stage though so it needs
      # to be checked before we start
      grouped <- ntsworkflow::keepConsecutive(
        alignment = grouped, 
        samples = sc,
        consecutive = gf(docsSrc, "nts_alig_filter_num_consecutive", numeric(1), justone = T)  
      )
    },
    replicate = {
      # Uses regexp to collect replicate groups. If min_features is larger than
      # number of replicates (e.g. failed meas. files), min_features will be 
      # automatically reduced.
      # This is a round-about way of doing this. To save the regex in the
      # database and then use this in the code is risky. It would be better to
      # save the replicate groups directly in the index (analogously to the 
      # way this is done for blanks)
      
      rgx <- gf(docsSrc[sc], "dbas_replicate_regex", character(1), justone = T)
      mf <- gf(docsSrc[sc], "nts_alig_filter_min_features", numeric(1), justone = T)
      grouped <- ntsworkflow::keep_reps_regex(
        alignment = grouped,
        samples = sc,
        sampleList = sampleList,
        regexp = rgx,
        least = mf
      )
      # TODO need to get rid of nts_alig_filter_num_replicates in msrawfiles 
    },
    min_features = {
      # removeRare uses all samples (including blanks)
      minFeat <- gf(docsSrc, "nts_alig_filter_min_features", numeric(1), justone = T)
      # Check that min_feat are not more than the number of samples still in batch
      if (minFeat <= nrow(sampleList)) {
        grouped <- ntsworkflow::removeRare(grouped, minFeat)
      } else {
        log_warn("Unable to use min_features on batch {batchName}, not enough samples")
      }
    },
    {
      log_error("nts_alig_filter_type must be one of consecutive, replicate, min_features")
      stop()
    }
  )
  
  aligRowsAfterFilter <- nrow(grouped)
  
  # Annotation ####
  
  log_info("Starting annotation")
  meths <- gf(docsSrc, "chrom_method", character(1), justone = T)
  chromMethod <- ifelse(meths == "bfg_nts_rp1", 
                        "dx.doi.org/10.1016/j.chroma.2015.11.014", meths) 
  
  # TODO change ntsworkflow: instrument: default settings should be to include everything
  # TODO change ntsworkflow: add parallelization to annotate_grouped
  annotationTable <- ntsworkflow::annotate_grouped(  
    sampleListLocal = sampleList,
    peakListList = peaklist,
    alignmentTable = grouped,
    db_path = dbPath,  
    threshold_score = gf(docsSrc, "nts_annotation_threshold_dp_score", numeric(1), justone = T),
    mztolu = gf(docsSrc, "nts_annotation_mz_tol", numeric(1), justone = T) / 1000,
    rttol = gf(docsSrc, "nts_annotation_rt_tol", numeric(1), justone = T),
    polarity = gf(docsSrc, "pol", character(1), justone = T),
    CE = c(
      gf(docsSrc, "nts_annotation_ce_min", numeric(1), justone = T),
      gf(docsSrc, "nts_annotation_ce_max", numeric(1), justone = T)
    ),
    CES = c(
      gf(docsSrc, "nts_annotation_ces_min", numeric(1), justone = T),
      gf(docsSrc, "nts_annotation_ces_max", numeric(1), justone = T)
    ),
    instrument = unlist(gf(docsSrc, "dbas_instr", "unknown", j = T)),
    mztolu_ms2 = gf(docsSrc, "nts_annotation_ms2_mz_tol", numeric(1), justone = T) / 1000,
    rtoffset = gf(docsSrc, "nts_annotation_rt_offset", numeric(1), justone = T),
    intCutData = gf(docsSrc, "nts_annotation_int_cutoff", numeric(1), justone = T),
    numcores = 1,
    datenListLocal = datenList,
    chrom_method = chromMethod,
    expGroups = specSource
  ) 
  
  log_info("Completed annotation")
  
  stopifnot(!is.null(annotationTable))
  # Set IS peaks ####
  log_info("Setting IS peaks")
  intStd <- gf(docsSrc, "dbas_is_name", character(1), justone = T)
  aligIdIs <- subset(annotationTable, name == intStd, alignmentID, drop = TRUE)
  if (length(aligIdIs) > 1) {
    # keep only highest row
    intCols <- grouped[grouped[, "alignmentID"] %in% aligIdIs, 
                       c(grep("Int_", colnames(grouped)), grep("alignmentID", colnames(grouped))), drop = F]
    intColsSum <- apply(intCols[, grep("Int_", colnames(intCols)), drop = F], 1, sum)
    aligIdIs <- intCols[which.max(intColsSum), "alignmentID"]
  }
  stopifnot(is.numeric(aligIdIs), length(aligIdIs) == 1)
  isPids <- grouped[
    grouped[, "alignmentID"] == aligIdIs, 
    grep("PeakID", colnames(grouped)),
    drop = TRUE
  ]
  # Sometimes IS peak is not found in all samples, for whatever reason the
  # peak isn't picked. In this case you need to estimate the intensity by
  # interpolation (see section Extract IS intensities and areas).
  
  sampleList$normalizePeakID <- isPids
  if (any(isPids == 0)) {
    b <- sampleList[sampleList$normalizePeakID == 0, "File", drop = T]
    log_warn("No IS found in sample(s) {paste(b, collapse = ', ')} ")
  }
  
  # Blank correction ####
  
  log_info("Starting blank correction")
  rowsBeforeBlank <- nrow(grouped)
  if (any(sampleList$sampleType == "Blank")) {
    grouped <- ntsworkflow::blankCorrection(
      alignment = grouped, 
      samplels = sampleList, 
      intensityFactor = gf(docsSrc, "nts_blank_correction_factor", numeric(1), justone = T),  
      deleteGrouped = FALSE  # This needs to be tested if it really works before setting it to true
    )
  }
  
  log_info("Completed blank correction")
  
  # clear the annotation table for rows which are no longer in the alignment table
  # after blank correction
  annotationTable <- subset(annotationTable, alignmentID %in% grouped[, "alignmentID"])
  
  # Package results into a proc_output list ####
  
  procoOb <- list(
    peaklist = peaklist, 
    peakPickSettings = peakPickSettings, 
    datenList = datenList, 
    sampleList = sampleList,
    grouped = grouped,
    annotationTable = annotationTable,
    docsSource = docsSrc
  )
  
  class(procoOb) <- c("proco_nts", "proco", class(procoOb))
  
  # Print some statistics
  log_info("--- Stats ---")
  log_info("Number of samples processed: {length(docsSrc)}")
  log_info("Alignment features uncorrected: {aligRowsUncorrected}")
  log_info("Correction by filter: {round(100*(1-(aligRowsAfterFilter/aligRowsUncorrected)))}%")
  log_info("Correction by blanks: {round(100*nrow(grouped)/rowsBeforeBlank)}%")
  log_info("Annotated features: {round(100*nrow(annotationTable)/nrow(grouped))}%")
  
  procoOb
}

#' Extract chromatograms and spectra on a file-wise basis
#'
#' @description
#' Takes a list of ntsp docs organised by sample and extracts chromatograms
#' and spectra from the rawfiles and attaches these to the docs. This is
#' only for the nts processing side becuase it relies on the peaklist
#' 
#' @param docsOneFile list of docs all from one measurement file
#' @param peakListLong peak list in long format (from proco object)
#' @param eicExtractWidth extraction width to get EIC (Da)
#'
#' @details
#' This function is slow because the file must be opened and a lot of loops need
#' to be made to extract peaks.
#' 
#'
#' @return list of docs with spectra and chromatograms included (when available)
#'
extract_spec_file <- function(docsOneFile, peakListLong, eicExtractWidth) {
  
  # First, we simply open the file, this is done outside the main loop
  # so that it is only done once per sample.
  # 
  # aligSamp <- bySamp[[1]]
  # 
  # Get the measurement file path and do some checks
  filePath <- unique(vapply(docsOneFile, function(doc) doc$path, character(1)))
  stopifnot(length(unique(filePath)) == 1, file.exists(filePath))
  log_info("Extracting spectra and EICs from {basename(filePath)}")
  # Open raw measurement file
  rawfile <- suppressMessages(xcms::xcmsRaw(filePath, includeMSn = T))
  
  # This is the main loop where we go through each feature (doc) in this sample
  # and extract the spectra and chromatogram
  extract_spec_doc <- function(doc, pl, eicExtractWidth) {
    
    # Collect eic
    tryCatch({
      lert <- pl[pl$peak_id_all == doc$PeakID, "LeftendRT"] - 20
      rert <- pl[pl$peak_id_all == doc$PeakID, "RightendRT"] + 20
      eic <- xcms::rawEIC(
        rawfile,
        mzrange = c(
          doc$mz - eicExtractWidth / 2, 
          doc$mz + eicExtractWidth / 2
        ),
        rtrange = c(lert, rert)
      )
      if (length(eic) > 0) {
        eic$time <- round(rawfile@scantime[eic$scan], 4)  # eic.time is in seconds!
        
        # reduce number of rows by calculating average intensities
        # want to have max 50 data points per peak
        reduction <- floor(length(eic$time) / 100)
        
        if (reduction < 1)  # if this is the case no need for reduction
          reduction <- 1
        if (reduction > 1) {
          oo <- zoo::zoo(cbind(eic$time, eic$int), eic$scan)
          oor <- zoo::rollapply(oo, reduction, mean, by = reduction)
          eicm <- cbind(time = as.numeric(oor[,1]), int = as.numeric(oor[,2]))  
        } else {
          eicm <- cbind(time = eic$time, int = eic$int)
        }
        
        # Make sure there are no values below 0
        eicm <- eicm[eicm[, "int"] > 0, , drop = FALSE]
        # Only add if you have at least 5 datapoints
        if (nrow(eicm) > 4) {
          eicm[, "int"] <- signif(eicm[, "int"], 5)
          
          # Convert to list
          eicl <- split(eicm, seq_len(nrow(eicm)))
          eicl <- lapply(eicl, as.list)
          eicl <- lapply(eicl, function(x) {names(x) <- c("time", "int"); x})
          doc[["eic"]] <- unname(eicl)
        }
      }
    },
    error = function(cnd) {
      log_warn("Error in EIC extraction in file: {doc$path}, PeakID: 
                  {doc$PeakID}. message: {conditionMessage(cnd)}")
    })
    
    # Collect MS1
    tryCatch({
      ms1scan <- pl[pl$peak_id_all == doc$PeakID, "Scan", drop = T][1]
      stopifnot(is.numeric(ms1scan), ms1scan != 0)
      ms1 <- as.data.frame(xcms::getScan(rawfile, ms1scan))
      
      if (nrow(ms1) > 0) {
        colnames(ms1) <- c("mz", "int")
        
        # Focus only on area of spec around mz of feature
        # TODO This is here set to +5 Da but needs adjustment!!!
        stopifnot(length(doc$mz) == 1, is.numeric(doc$mz))
        ms1 <- ms1[ms1$mz > doc$mz - 1 & ms1$mz < doc$mz + 5, , drop = F]
        
        # Remove noise
        # 1) remove anything below baseline intensity
        bl <- pl[pl$peak_id_all == doc$PeakID, "Baseline", drop = T][1]
        stopifnot(is.numeric(bl))
        ms1 <- ms1[ms1$int >= bl, , drop = F]
        
        # If everything is below baseline then that is very strange... since how
        # was the peak ever picked? If it is the case however, just forget the
        # MS1 entirely (otherwise the data becomes too large)
        if (nrow(ms1) > 0) {
          # Limit size to 50 top peaks
          if (nrow(ms1) > 50) {
            ms1 <- ms1[order(ms1$int, decreasing = T), ]
            ms1 <- ms1[1:50, ]
            ms1 <- ms1[order(ms1$mz), ]
          }
          ms1$mz <- round(ms1$mz, 4)
          ms1$int <- signif(ms1$int, 5)
          ms1 <- split(ms1, seq_len(nrow(ms1)))
          ms1 <- lapply(ms1, as.list)
          doc[["ms1"]] <- unname(ms1)
        }
      }
    },
    error = function(cnd) {
      log_warn("Error in MS1 extraction in file: {doc$path}, PeakID: 
                    {doc$PeakID}. message: {conditionMessage(cnd)}")
    })
    
    
    # Collect MS2
    tryCatch({
      ms2scan <- pl[pl$peak_id_all == doc$PeakID, "MS2scan", drop = T][1]
      #ms2Scan <- alig3b[alig3b$PeakID == doc$PeakID, "ms2scan", drop = T][1]
      stopifnot(is.numeric(ms2scan))
      if (ms2scan == 0)
        return(doc)
      
      ms2 <- as.data.frame(xcms::getMsnScan(rawfile, ms2scan))
      
      stopifnot(nrow(ms2) > 0)
      colnames(ms2) <- c("mz", "int")
      
      if (nrow(ms2) == 0)
        return(doc)
      # cut off anything above mz of precursor
      ms2 <- ms2[ms2$mz < doc$mz + 1, ]
      # In case there are a lot of fragments, cut out bottom 2% of intensities
      if (nrow(ms2) > 20) {
        noiseMS2 <- max(ms2$int)*0.02
        ms2 <- ms2[ms2$int > noiseMS2, ]
      }
      # Limit size to 50 top peaks
      if (nrow(ms2) > 50) {
        ms2 <- ms2[order(ms2$int, decreasing = T), ]
        ms2 <- ms2[1:50, ]
        ms2 <- ms2[order(ms2$mz), ]
      }
      # ggplot(ms2, aes(mz, 0, xend = mz, yend = int)) + geom_segment()
      ms2$mz <- round(ms2$mz, 4)
      ms2$int <- signif(ms2$int, 5)
      ms2 <- split(ms2, seq_len(nrow(ms2)))
      ms2 <- lapply(ms2, as.list)
      doc[["ms2"]] <- unname(ms2)
    },
    error = function(cnd) {
      log_warn("Error in MS2 extraction in file: {doc$path}, PeakID: 
                    {doc$PeakID}. message: {conditionMessage(cnd)}")
    })
    
    doc
    
  }
  
  # Invocation of main loop
  docsOneFileNew <- lapply(docsOneFile, extract_spec_doc, pl = peakListLong, 
                           eicExtractWidth = eicExtractWidth)
  
  # Reduce size of rawFile (for some reason garbage collection does not properly
  # clear memory)
  rawfile@env$intensity <- NULL
  rawfile@env$mz <- NULL
  rawfile@env$profile <- NULL
  rawfile@env$msnIntensity <- NULL
  rawfile@env$msnMz <- NULL
  
  docsOneFileNew
  
}

#' @export
make_ntspl.proco_nts <- function(proco, coresBatch = 1) { 
  log_info("Building ntspl object")
  # Create list form of proc_output for saving as json ####
  
  # These are spec sources which pollute the comp_group field and need to be removed
  # TODO change structure of CSL so that data sources are not mixed with compound groups
  SPEC_SOURCES <- c("BfG", "LfU", "UBA")
  # These fields are copied from the docs to the results
  FIELDS_MERGE <- c(
    "start", 
    "station",
    "matrix",
    "loc", 
    "river", 
    "gkz", 
    "km",
    "duration", 
    "data_source",
    "sample_source",
    "licence",
    "date_measurement",
    "chrom_method",
    "instrument",
    "pol",
    "tag",
    "comment",
    "path"
  )
  
  # Clean alignment table ####
  alig <- as.data.frame(proco$grouped)
  annot <- proco$annotationTable
  samp <- proco$sampleList
  pll <- proco$peaklist
  pl <- do.call("rbind", pll)
  docsSrc <- proco$docsSource
  rm(proco, pll)
  
  # Merge annotation and alig tables
  annot2 <- annot[, c("alignmentID", "name", "CAS", "formula", "SMILES", "adduct")]
  alig <- merge(alig, annot2, by = "alignmentID", all.x = T)
  alig <- subset(alig, , -c(alignmentID, MS2Fit))
  # Go through alig and create doc for every row for each sample. 
  
  # each doc mz, rt, intensity, area, eic, ms1 and ms2 for each sample. 
  # All the mz of the Gruppe will make up the ms1 spec. 
  # Make alig table long
  alig2 <- tidyr::pivot_longer(alig, matches("_\\d+$"), names_sep = "_", 
                               names_to = c(".value", "sample"))
  alig2 <- alig2[alig2$Int != 0, ]
  alig2$gruppe <- NULL
  alig2$mean_mz <- NULL
  alig2$mean_RT <- NULL
  alig2$RT <- round(alig2$RT / 60, 2)
  samp2 <- samp[, c("ID", "File")]
  alig2 <- merge(alig2, samp2, by.x = "sample", by.y = "ID", all.x = T)
  alig2$File <- basename(alig2$File)
  alig2 <- alig2 %>% 
    rename(rt = RT, intensity_from_alig = Int, cas = CAS, smiles = SMILES, 
           filename = File)
  
  plArea <- pl[, c("Area", "Intensity", "peak_id_all")]
  alig2 <- merge(alig2, plArea, by.x = "PeakID", by.y = "peak_id_all", all.x = TRUE, sort = FALSE)
  alig2 <- alig2 %>% rename(area = Area, intensity = Intensity)
  # Remove blank samples
  nonBlanks <- basename(samp[samp$sampleType == "Unknown", "File"])
  alig2 <- alig2[is.element(alig2$filename, nonBlanks), ]
  
  # Extract IS intensities and areas ####
  
  stopifnot("normalizePeakID" %in% colnames(samp))
  get_is_inten_area <- function(sampleNums, iora) {
    peakIds <- vapply(
      sampleNums, 
      function(x) samp[samp$ID == x, "normalizePeakID"], 
      numeric(1)
    )
    vapply(
      peakIds, 
      function(x) {
        y <- pl[pl$peak_id_all == x, iora]
        ifelse(is.null(y), NA, y)
      }, numeric(1))
  } 
  sids <- as.numeric(unique(alig2$sample))
  is_alig <- data.frame(
    sample = sids, 
    intensity_is = get_is_inten_area(sids, "Intensity"), 
    area_is = get_is_inten_area(sids, "Area"), 
    name_is = gf(docsSrc, "dbas_is_name", character(1), justone = T)
  )
  
  # Fill NAs with averages
  # We assume an error in the peak-picking has occurred and there is no IS peak
  # TODO This needs to be considered again, what is the point of IS if you are
  # just going to ignore a missing IS peak?
  if (any(is.na(is_alig$intensity_is)) || any(is.na(is_alig$area_is))) {
    log_warn("Ignoring missing IS and taking the average intensity of the other files")
    i <- mean(is_alig$intensity_is, na.rm = T)
    a <- mean(is_alig$area_is, na.rm = T)
    is_alig[is.na(is_alig$intensity_is), "intensity_is"] <- i
    is_alig[is.na(is_alig$area_is), "area_is"] <- a
  }
  
  alig3 <- merge(alig2, is_alig, by = "sample")
  
  # Remove FP annotations ####
  # Internal standards have been removed by the blank correction.
  # False positives from PP have been removed by the alignment table filters
  # At this stage we are just removing incorrect annotations, the peaks
  # themselves are still okay.
  fpRow <- alig3$name %in% unique(unlist(gf(docsSrc, "dbas_fp", "unknown")))
  alig3[fpRow, c("name", "cas", "formula", "smiles", "adduct")] <- NA
  
  # Add component information
  # A component is identified by ID, the number is made of Unix time stamp,
  # sample number in the batch and combined group number from alig
  numGr <- table(alig3$Gruppe, alig3$sample)
  batchId <- round(as.numeric(Sys.time()))  # use unix time as id for batch
  alig3$component <- vapply(seq_len(nrow(alig3)), function(i) {
    gr <- alig3[i, "Gruppe"]
    sp <- alig3[i, "sample"]
    if (gr == 0 || numGr[as.character(gr), as.character(sp)] <= 1) {
      return(NA)
    } else {
      return(as.numeric(paste0(batchId, sp, gr)))
    }
  }, numeric(1))
  alig3$Gruppe <- NULL
  
  # Compute normalized intensity and area ####
  if (all(c("intensity", "intensity_is") %in% colnames(alig3)) && all(alig3$intensity_is != 0))
    alig3$intensity_normalized <- alig3$intensity / alig3$intensity_is
  if (all(c("area", "area_is") %in% colnames(alig3)) && all(alig3$area_is != 0))
    alig3$area_normalized <- alig3$area / alig3$area_is
  
  alig3$intensity_from_alig <- NULL
  alig3$sample <- as.numeric(alig3$sample)
  
  # Determine which Isotopologue has been annotated
  # Assume that for one compound, these have distinct nominal masses.
  # Use the name, nominal mass and adduct to determine the isotope.
  # TODO this should be incorporated into ntsworkflow add isotopolgue info
  # should be annotated in annotate_grouped
  alig3$nom_mass <- round(alig3$mz)
  dbPath <- gf(docsSrc, "nts_spectral_library", character(1), justone = T)
  thispol <- gf(docsSrc, "pol", character(1), justone = T)
  sdb <- con_sqlite(dbPath)
  
  comptab <- tbl(sdb, "experiment") %>% 
    left_join(tbl(sdb, "parameter"), by = "parameter_id") %>%
    filter(polarity == !!thispol) %>% 
    left_join(tbl(sdb, "compound"), by = "compound_id") %>%
    select(name, mz, adduct, isotope) %>% collect() %>% 
    rename(isotopologue = isotope) %>% 
    mutate(nom_mass = round(mz)) %>%
    select(-mz) %>% 
    distinct()
  alig3b <- merge(alig3, comptab, by = c("name", "adduct", "nom_mass"), all.x = T)
  alig3b$nom_mass <- NULL
  DBI::dbDisconnect(sdb)
  rm(comptab, alig3)
  
  # Round some of the numbers to reduce file size
  
  alig3b$mz <- round(alig3b$mz, 4)
  alig3b$area <- signif(alig3b$area, 5)
  alig3b$intensity <- signif(alig3b$intensity, 5)
  alig3b$intensity_is <- signif(alig3b$intensity_is, 5)
  alig3b$area_is <- signif(alig3b$area_is, 5)
  alig3b$intensity_normalized <- signif(alig3b$intensity_normalized, 5)
  alig3b$area_normalized <- signif(alig3b$area_normalized, 5)
  alig3b$area_is <- signif(alig3b$area_is, 5)
  
  
  # Convert to list ####
  alig4 <- split(alig3b, seq_len(nrow(alig3b)))
  alig4 <- lapply(alig4, as.list)
  
  # Add further compound information formula, inchi and inchikey
  # Add comp_group
  
  sdb <- con_sqlite(dbPath)
  comptab <- tbl(sdb, "compound") %>% collect()
  grouptab <- tbl(sdb, "compound") %>% 
    select(name, compound_id) %>% 
    left_join(tbl(sdb, "compGroupComp"), by = "compound_id") %>% 
    left_join(tbl(sdb, "compoundGroup"), by = "compoundGroup_id") %>% 
    select(name.x, name.y) %>% 
    rename(compname = name.x, groupname = name.y) %>% 
    collect()
  DBI::dbDisconnect(sdb)
  # Using '$' will fail without an error because it implements greedy matching 
  # (inchi and inchikey)
  alig4 <- lapply(alig4, function(doc) {
    if(!is.na(doc$name)) {
      doc[["inchi"]] <- filter(comptab, name == !!doc$name) %>% pull(inchi)
      doc[["inchikey"]] <- filter(comptab, name == !!doc$name) %>% pull(inchikey)
      doc[["formula"]] <- filter(comptab, name == !!doc$name) %>% pull(formula)
      cg <- filter(grouptab, compname == !!doc$name) %>% pull(groupname)
      cg <- cg[!is.element(cg, SPEC_SOURCES)]
      doc[["comp_group"]] <- cg
    }
    doc
  })
  
  
  # Remove duplicate peaks ####
  # duplicated peaks, due to multiple annotations, need to be combined and a vector
  # of annotations needs to be made.
  peakIds <- vapply(alig4, function(doc) doc$PeakID, numeric(1)) 
  dupPeakIds <- peakIds[duplicated(peakIds)]
  # Need to create a safe way of extracting elements, such that this does not 
  # fail when elements are missing from the list.
  # This function will also remove any NA values, so that these are not added to
  # array.
  safe_extract <- function(docs, field, parser) {
    l <- purrr::map_if(
      .x = docs, 
      .p = function(y) is.element(field, names(y)), 
      .f = function(y) y[[field]], 
      .else = function(y) NA
    )
    # need to check if any is na, because sometimes each
    # element in the list has more than one entry
    l <- purrr::discard(l, function(y) any(is.na(y)))
    parser(l)
  } 
  # make combined docs
  combinedDocs <- lapply(dupPeakIds, function(x) {  #  x <- dupPeakIds[200]
    # ERROR HERE - sometimes we have no inchi?
    toCombi <- alig4[vapply(alig4, function(doc) doc$PeakID == x, logical(1))]
    newDoc <- toCombi[[1]]
    if (!is.na(newDoc$name)) {
      newDoc[["name"]] <- safe_extract(toCombi, "name", as.character)
      newDoc[["cas"]] <- safe_extract(toCombi, "cas", as.character)
      newDoc[["formula"]] <- safe_extract(toCombi, "formula", as.character)
      newDoc[["smiles"]] <- safe_extract(toCombi, "smiles", as.character)
      newDoc[["adduct"]] <- safe_extract(toCombi,"adduct", as.character)
      newDoc[["inchi"]] <- safe_extract(toCombi, "inchi", as.character)
      newDoc[["inchikey"]] <- safe_extract(toCombi, "inchikey", as.character)
      cg <- unique(unlist(safe_extract(toCombi, "comp_group", as.list)))
      newDoc[["comp_group"]] <- cg
    }
    newDoc
  })
  rm(peakIds)
  # remove old docs
  alig5 <- Filter(function(doc) !is.element(doc$PeakID, dupPeakIds), alig4) 
  # add combined docs
  alig6 <- c(alig5, combinedDocs)
  rm(alig4, alig5)
  
  
  # Attach sample information ####
  alig6 <- lapply(alig6, function(doc) { # doc <- alig6[[1]]
    sampdoc <- docsSrc[[doc$sample]]
    toMerge <- lapply(FIELDS_MERGE, function(x) sampdoc[[x]])
    names(toMerge) <- FIELDS_MERGE
    toMerge <- purrr::discard(toMerge, is.null)
    c(doc, toMerge)
  })
  
  # Clean up environment ####
  rm(list = ls()[!is.element(ls(), c(
    "alig6", "pl", "coresBatch", "samp", "rfindex", "eicExtraction", "docsSrc"))])
  
  # Add rtt (retention time table)
  alig6 <- lapply(alig6, function(doc) {
    newEntry <- list(
      method = doc$chrom_method,
      predicted = FALSE,
      rt = doc$rt
    )
    doc$rtt <- list(newEntry)
    doc
  })
  
  # Collect EIC, MS1 and MS2 ####
  log_info("Collecting chromatograms and spectra")
  bySamp <- split(alig6, sapply(alig6, function(doc) doc$sample))
  eicExtraction <- gf(docsSrc, "nts_eic_extraction_width", numeric(1), justone = T) / 1000
  bySamp2 <- lapply(bySamp, extract_spec_file, peakListLong = pl, eicExtractWidth = eicExtraction)
  alig7 <- do.call("c", bySamp2)
  rm(bySamp, bySamp2, alig6)
  
  # Remove NAs
  alig7 <- lapply(alig7, function(doc) doc[names(which(!is.na(doc)))])
  
  # Remove zero length values
  alig7 <- lapply(alig7, function(doc) purrr::keep(doc, function(f) length(f) > 0))
  # remove fields
  alig7 <- lapply(alig7, function(doc) {
    doc$sample <- NULL
    doc$PeakID <- NULL
    doc$ms2scan <- NULL
    doc
  })
  
  ntsplOb <- unname(alig7)
  
  class(ntsplOb) <- c("ntspl_nts", "ntspl", class(ntsplOb))
  attr(ntsplOb, "nts_alias_name") <- gf(docsSrc, "nts_alias_name", character(1), justone = T)
  attr(ntsplOb, "nts_index_name") <- gf(docsSrc, "nts_index_name", character(1), justone = T)
  ntsplOb
}
  

#' @export
save_ntspl.ntspl_nts <- function(ntspList, saveDir, maxSizeGb = 10) {
  
  # If ntsplList is too large, cannot write this to json (exceeds R's string
  # object limit)
  # It is estimated that an ntspl object of >10 will create a string that is
  # too large for R to hold.
  
  # Get the memory size in GB
  ntsplSize <- round(as.numeric(object.size(ntspList)) / 1000000000)
  
  # How many times is the object bigger that maxSizeGb, we have to split the object
  # by this number and each part has to be saved as a separate json
  if (ntsplSize > maxSizeGb) {
    # At this point, part should not be in the attributes, otherwise something
    # went wrong
    stopifnot(!is.element("part", names(attributes(ntspList))))
    # How many splits needed
    splitBy <- ceiling(ntsplSize / maxSizeGb)
    splitNames <- letters[1:splitBy]
    splitFac <- rep(splitNames, length.out = length(ntspList))
    # We have to add the part name and the old attributes back to each part
    prevAttributes <- attributes(ntspList)
    prevClass <- class(ntspList)
    # Split up the list and overwrite the memory location
    ntsplListSplit <- split(ntspList, splitFac)
    rm(ntspList)
    ntsplListSplit <- mapply(
      function(theList, thePart, theAtt, theClass) {
        attributes(theList) <- theAtt
        attr(theList, "part") <- thePart
        class(theList) <- theClass
        theList
      }, 
      ntsplListSplit, 
      splitNames, 
      MoreArgs = list(theAtt = prevAttributes, theClass = prevClass),
      SIMPLIFY = FALSE
    )
    # for each of these parts, run this function again
    savenm <- lapply(ntsplListSplit, save_ntspl, saveDir = saveDir, maxSizeGb = maxSizeGb)
    return(as.character(savenm))
  } 
  
  # If the "part" is not yet set, this means the list is the only part so it 
  # can be set to "a" 
  if (!is.element("part", names(attributes(ntspList))))
    attr(ntspList, "part") <- "a"
  
  savename <- gsub("/", "_", dirname(ntspList[[1]][["path"]]))
  savename <- gsub("\\.", "_", savename)
  # Save the index into which the docs must be ingested into the filename
  savename <- paste0(
    "nts-batch--", 
    "inda-",
    attr(ntspList, "nts_alias_name"),
    "-indn-",
    attr(ntspList, "nts_index_name"),
    "-bi-",
    savename,
    "-part-",
    attr(ntspList, "part"),
    "--", 
    format(Sys.time(), "%y%m%d"), 
    ".json"
  )
  savename <- file.path(saveDir, savename)
  
  
  log_info("Writing JSON file {savename}")
  #jsonlite::write_json(ntspList, savename, auto_unbox = T, pretty = T)
  # rjson seems much faster that jsonlite
  tryCatch({
    jsonString <- rjson::toJSON(ntspList, indent = 2)
    writeLines(jsonString, savename)
  },
  error = function(cnd) {
    log_error("In writing json {savename} returned: {conditionMessage(cnd)}")
    stop()
  })
  
  log_info("Completed JSON file {savename}")
  savename
} 
  



