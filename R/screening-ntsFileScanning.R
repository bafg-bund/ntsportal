
#' Scan files by non-target-screening processing
#'
#' @param msrawfilesRecords Files to process as a `list` of `msrawfileRecord`s
#' @param forNtsworkflow logical, should the results be prepared for the ntsworkflow non-target app?
#'
#' @returns an `ntsResult` object with the tables `peakList`, `sampleList`, `alignmentTable`, `annotationTable`
scanBatchNts <- function(msrawfilesRecords, forNtsworkflow = FALSE) {
  peakPickingResults <- purrr::map(msrawfilesRecords, getPeakPickingResult)
  peakPickingFails <- getPeakPickingFails(peakPickingResults)
  peakPickingResults <- peakPickingResults[!peakPickingFails]
  msrawfilesRecordsNew <- msrawfilesRecords[!peakPickingFails]
  if (all(peakPickingFails)) 
    return(getEmptyNtsResult(msrawfilesRecords))
  
  peaklist <- map(peakPickingResults, \(x) x$finishedPeakList)
  datenList <- map(peakPickingResults, \(x) x$rawFile)
  peakPickSettings <- map(peakPickingResults, \(x) x$peakPickSettings)
  sampleList <- createSampleList(msrawfilesRecordsNew)
  peaklist <- ntsworkflow::new_peaklist(peaklist, datenList)
  peaklist <- lapply(peaklist, function(x) x[order(x$mz),])
  rm(peakPickingResults)
  
  grouped <- getAlignmentTable(peaklist, msrawfilesRecordsNew)
  idsOfUnknowns <- sampleList[sampleList$sampleType == "Unknown", "ID"]
  recFirstUnknown <- msrawfilesRecordsNew[[idsOfUnknowns[1]]]
  grouped <- filterAlignmentTable(grouped, sampleList, idsOfUnknowns, recFirstUnknown)
  if (nrow(grouped) == 0) 
    return(getEmptyNtsResult(msrawfilesRecords))
  annotationTable <- getAnnotationTable(sampleList, peaklist, grouped, datenList, recFirstUnknown)
  
  intStdName <- recFirstUnknown$dbas_is_name
  sampleList <- addIntStdPeaksToSampleList(sampleList, annotationTable, grouped, intStdName)
  
  if (any(sampleList$sampleType == "Blank")) {
    grouped <- removeBlankRowsFromAlignmentTable(grouped, sampleList, recFirstUnknown$nts_blank_correction_factor)
    annotationTable <- removeResidualRowsAnnotation(annotationTable, grouped)
  }
  if (forNtsworkflow) {
    return(
      list(peaklist = peaklist, peakPickSettings = peakPickSettings, datenList = datenList, sampleList = sampleList,
           grouped = grouped, annotationTable = annotationTable)
    )
  }
  makeNtsResult(peaklist, sampleList, grouped, annotationTable)
}


createSampleList <- function(records) {
  aliasName <- map_chr(records, \(x) x$feature_table_alias)
  eicExtractionWidth <- map_dbl(records, \(x) x$nts_eic_extraction_width)
  data.frame(
    ID = seq_along(records),
    File = map_chr(records, \(x) x$path),
    sampleType = ifelse(map_lgl(records, \(x) x$blank), "Blank", "Unknown"),
    optMzStep = map_dbl(records, \(x) x$nts_mz_step),
    RAM = FALSE,
    deleted = FALSE,
    featureAliasName = aliasName,
    eicExtractionWidth = eicExtractionWidth
  )
}

getPeakPickingResult <- function(msrawfileRecord) {
  rawFile <- loadFileXcms(msrawfileRecord$path)
  if (!exists("rawFile")) 
    return(getEmptyPeakPickingResult(msrawfileRecord))
  rawPeakList <- getRawPeakList(rawFile, msrawfileRecord)
  if (!exists("rawPeakList") || nrow(rawPeakList) == 0) 
    return(getEmptyPeakPickingResult(msrawfileRecord))
  componentizedPeakList <- getComponentizedPeakList(rawPeakList, msrawfileRecord)
  if (!exists("componentizedPeakList")) 
    return(getEmptyPeakPickingResult(msrawfileRecord))
  finishedPeakList <- addRealPeakColumn(componentizedPeakList)  # unknown usage
  rawFile <- reduceRawFileSize(rawFile)
  peakPickSettings <- getPeakPickSettingsForNtsApp(msrawfileRecord)
  makePeakPickingResult(finishedPeakList, rawFile, peakPickSettings)
}

getAlignmentTable <- function(peaklist, records) {
  grouped <- ntsworkflow::alignment_BfG_cpp(
    peaklists = peaklist,
    mz_dev = records[[1]]$nts_alig_delta_mz,
    DeltaRT = records[[1]]$nts_alig_delta_rt,
    mz_dev_unit = "mDa"
  )
  # Collect componentization information from samples to column "Gruppe"
  grouped <- ntsworkflow::summarize_groups(grouped)
  
  # Add alignment ID column
  oldnames <- colnames(grouped)
  grouped <- cbind(grouped, seq_len(nrow(grouped)))
  colnames(grouped) <- c(oldnames, "alignmentID")
  grouped
}

filterAlignmentTable <- function(grouped, sampleList, idsOfUnknowns, recFirstUnknown) {
  # Consecutive, triplicate injection or minimum features filter
  # The filtering only applies to non-blanks ("unknowns" in ntsworkflow)
  # (expect for min_features, this applies to all samples)
  filterType <- recFirstUnknown$nts_alig_filter_type  
  log_info("Alignment table filter of type {filterType}")
  switch(
    filterType,
    consecutive = {
      # For this to work the samples must be ordered by start (sample time).
      grouped <- ntsworkflow::keepConsecutive(
        alignment = grouped,
        samples = idsOfUnknowns,
        consecutive = recFirstUnknown$nts_alig_filter_num_consecutive
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
      
      grouped <- ntsworkflow::keep_reps_regex(
        alignment = grouped,
        samples = idsOfUnknowns,
        sampleList = sampleList,
        regexp = recFirstUnknown$dbas_replicate_regex,
        least = recFirstUnknown$nts_alig_filter_min_features
      )
    },
    min_features = {
      # removeRare uses all samples (including blanks)
      # Check that min_feat are not more than the number of samples still in batch
      if (recFirstUnknown$nts_alig_filter_min_features <= nrow(sampleList)) {
        grouped <- ntsworkflow::removeRare(grouped, recFirstUnknown$nts_alig_filter_min_features)
      } else {
        log_warn("Unable to use min_features on batch {dirname(recFirstUnknown$path)}, not enough samples")
      }
    },
    {
      stop("nts_alig_filter_type must be one of consecutive, replicate, min_features, batch ", dirname(recFirstUnknown$path))
    }
  )
  grouped
}

getAnnotationTable <- function(sampleList, peaklist, grouped, datenList, recFirstUnknown) {
  log_info("Starting annotation")
  # TODO change ntsworkflow: instrument: default settings should be to include everything
  sdb <- connectSqlite(recFirstUnknown$spectral_library_path)
  specSource <- tbl(sdb, "experimentGroup") %>%
    select(name) %>% distinct() %>% collect() %>% .$name
  DBI::dbDisconnect(sdb)
  
  annotationTable <- ntsworkflow::annotate_grouped(
    sampleListLocal = sampleList,
    peakListList = peaklist,
    alignmentTable = grouped,
    db_path = recFirstUnknown$spectral_library_path,
    threshold_score = recFirstUnknown$nts_annotation_threshold_dp_score,
    mztolu = recFirstUnknown$nts_annotation_mz_tol,
    rttol = recFirstUnknown$nts_annotation_rt_tol,
    polarity = recFirstUnknown$pol,
    CE = c(
      recFirstUnknown$nts_annotation_ce_min,
      recFirstUnknown$nts_annotation_ce_max
    ),
    CES = c(
      recFirstUnknown$nts_annotation_ces_min,
      recFirstUnknown$nts_annotation_ces_max
    ),
    instrument = unlist(recFirstUnknown$dbas_instr),
    mztolu_ms2 = recFirstUnknown$nts_annotation_ms2_mz_tol / 1000,
    rtoffset = recFirstUnknown$nts_annotation_rt_offset,
    intCutData = recFirstUnknown$nts_annotation_int_cutoff,
    numcores = 1,
    datenListLocal = datenList,
    chrom_method = recFirstUnknown$chrom_method,
    expGroups = specSource
  )
  
  log_info("Completed annotation")
  annotationTable
}

addIntStdPeaksToSampleList <- function(sampleList, annotationTable, grouped, intStdName) {
  
  aligIdIs <- subset(annotationTable, name == intStdName, alignmentID, drop = TRUE)
  if (length(aligIdIs) > 1) {
    # keep only highest row
    intCols <- grouped[grouped[, "alignmentID"] %in% aligIdIs,
                       c(grep("Int_", colnames(grouped)), grep("alignmentID", colnames(grouped))), drop = F]
    intColsSum <- apply(intCols[, grep("Int_", colnames(intCols)), drop = F], 1, sum)
    aligIdIs <- intCols[which.max(intColsSum), "alignmentID"]
  }
  else if (length(aligIdIs) < 1) {
    aligIdIs = 0
  }
  
  stopifnot(is.numeric(aligIdIs))
  
  # Extract IS peak ids from grouped table
  if (aligIdIs != 0) {
    isPids <- grouped[
      grouped[, "alignmentID"] == aligIdIs,
      grep("PeakID", colnames(grouped)),
      drop = TRUE
    ]
  } else {
    isPids = 0
  }
  
  # Sometimes IS peak is not found in all samples, for whatever reason the
  # peak isn't picked. In this case you need to estimate the intensity by
  # interpolation (see section Extract IS intensities and areas).
  
  sampleList$normalizePeakID <- isPids
  sampleList$intStdName <- intStdName
  if (any(isPids == 0)) {
    b <- sampleList[sampleList$normalizePeakID == 0, "File", drop = T]
    log_warn("No IS found in sample(s) {paste(b, collapse = ', ')} ")
  }
  sampleList
}

removeBlankRowsFromAlignmentTable <- function(grouped, sampleList, blankCorrectionFactor) {
  ntsworkflow::blankCorrection(
    alignment = grouped,
    samplels = sampleList,
    intensityFactor = blankCorrectionFactor,
    deleteGrouped = FALSE  # This needs to be tested if it really works before setting it to true
  )
}

removeResidualRowsAnnotation <- function(annotationTable, grouped) {
  subset(annotationTable, alignmentID %in% grouped[, "alignmentID"])
}

loadFileXcms <- function(pathToMeasFile) {
  tryCatch(
    suppressMessages(xcms::xcmsRaw(pathToMeasFile, includeMSn = TRUE)),
    error = function(cnd) {
      log_warn("Error in loadFileXcms for file {pathToMeasFile}: {conditionMessage(cnd)}")
    }
  )
}

getEmptyPeakPickingResult <- function(record) {
  makePeakPickingResult(getEmptyPeakList(), record$path, getPeakPickSettingsForNtsApp(record))
}

getEmptyPeakList <- function() {
  data.frame(mz = numeric())
}

makePeakPickingResult <- function(finishedPeakList, rawFile, peakPickSettings) {
  list(finishedPeakList = finishedPeakList, rawFile = rawFile, peakPickSettings = peakPickSettings)
}

getRawPeakList <- function(rawFile, record) {
  tryCatch(
    ntsworkflow::pickPeaksMzRange(
      daten = rawFile,
      mz_min = record$nts_mz_min,
      mz_max = record$nts_mz_max,
      mz_step = record$nts_mz_step,
      rt_min = record$nts_rt_min * 60,
      rt_max = record$nts_rt_max * 60,
      sn = record$nts_sn,
      int_threshold = record$nts_int_threshold,
      peak_NoiseScans = record$nts_peak_noise_scans,
      precursormzTol = record$nts_precursor_mz_tol,
      peakwidth_min = record$nts_peak_width_min,
      peakwidth_max = record$nts_peak_width_max,
      maxPeaksPerSignal = record$nts_max_num_peaks
    ),
    error = function(cnd) {
      log_warn("Error in getRawPeakList for file {record$path}: {conditionMessage(cnd)}")
    }
  )
}

getComponentizedPeakList <- function(rawPeakList, record) {
  tryCatch(
    ntsworkflow::componentization_BfG(
      Liste = rawPeakList,
      daten = rawFile,
      ppm = record$nts_componentization_ppm,
      Grenzwert_RT = record$nts_componentization_rt_tol,
      Grenzwert_FWHM_left = record$nts_componentization_rt_tol_l,
      Grenzwert_FWHM_right = record$nts_componentization_rt_tol_r,
      Summe_all = record$nts_componentization_rt_tol_sum,
      adjust_tolerance = record$nts_componentization_dynamic_tolerance
    ),
    error = function(cnd) {
      log_warn("Error in getComponentizedPeakList for file {record$path}: {conditionMessage(cnd)}")
    }
  )
}

addRealPeakColumn <- function(peakList) {
  transform(peakList, RealPeak = TRUE)
}

reduceRawFileSize <- function(rawFile) {
  rawFile@env$intensity <- NULL
  rawFile@env$mz <- NULL
  rawFile@env$profile <- NULL
  rawFile@env$msnIntensity <- NULL
  rawFile@env$msnMz <- NULL
  rawFile
}

getPeakPickSettingsForNtsApp <- function(record) {
  settings <- list()
  settings$massrange <- c(
    record$nts_mz_min,
    record$nts_mz_max
  )
  settings$mz_step <- record$nts_mz_step
  settings$rtrange <- c(
    record$nts_rt_min * 60,
    record$nts_rt_max * 60
  )
  settings$peakwidth <- c(
    record$nts_peak_width_min,
    record$nts_peak_width_max
  )
  
  settings$NoiseScans <- record$nts_peak_noise_scans
  settings$sn <- record$nts_sn
  settings$int_threshold <- record$nts_int_threshold
  settings$precursormzTol <- record$nts_precursor_mz_tol
  settings$ppm <- record$nts_componentization_ppm
  settings$RT_Tol <- record$nts_componentization_rt_tol
  settings$PPTableRow <- 1  # unknown usage
  settings$orderType <- list(list(0,"asc")) # unknown usage
  settings$TableLength <- 10  # unknown usage
  settings$displayStart <- 0  # unknown usage
  settings$maxNumPeaks <- record$nts_max_num_peaks
  settings
}

makeNtsResult <- function(peaklist, sampleList, grouped, annotationTable) {
  alig <- tidyr::pivot_longer(as_tibble(grouped), matches("_\\d+$"), names_sep = "_",
                              names_to = c(".value", "sample"))
  alig <- alig[alig$Int != 0, ]
  alig$gruppe <- NULL
  alig$MS2Fit <- NULL
  alig$mean_mz <- NULL
  alig$mean_RT <- NULL
  alig$RT <- round(alig$RT / 60, 2)
  alig <- rename(alig, rt = RT, intensity = Int, sampleId = sample, peakId = PeakID, alignmentId = alignmentID, componentId = Gruppe)
  alig$sampleId <- as.numeric(alig$sampleId)
  alig$intensity <- round(alig$intensity)
  
  sampleList <- rename(sampleList, sampleId = ID, path = File, normalizePeakId = normalizePeakID)
  sampleList <- as_tibble(sampleList)
  
  peakList <- list_rbind(map(peaklist, as_tibble))
  peakList <- select(peakList, mz, rt = RT, area = Area, intensity = Intensity, ms1scan= Scan, ms2scan = MS2scan, 
                     leftEndRt = LeftendRT, rightEndRt = RightendRT, baseline = Baseline, sampleId = sample_id, peakId = peak_id_all)
  peakList <- mutate(peakList, area = round(area), intensity = round(intensity))
  
  annotationTable <- rename(annotationTable, alignmentId = alignmentID, cas = CAS, smiles = SMILES, isotopologue = isotope)
  
  structure(list(
    peakList = peakList,
    sampleList = sampleList,
    alignmentTable = alig,
    annotationTable = annotationTable
  ), class = "ntsResult")
}

isPeakPickingResultEmpty <- function(peakPickingResult) {
  nrow(peakPickingResult$finishedPeakList) == 0
}

getBatchName <- function(msrawfilesRecords){
  dirname(msrawfilesRecords[[1]]$path)
}

getEmptyNtsResult <- function(records) {
  list() 
}

getPeakPickingFails <- function(peakPickingResults) {
  map_lgl(peakPickingResults, isPeakPickingResultEmpty)
}
