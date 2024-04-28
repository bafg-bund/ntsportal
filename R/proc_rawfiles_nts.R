

# Functions for processing msrawfiles entries for nts



#' Process a batch of files using ntsworkflow
#'
#' @param escon 
#' @param rfindex 
#' @param esids 
#' @param tempsavedir 
#' @param ingestpth 
#' @param configfile 
#' @param coresBatch 
#' @param noIngest 
#'
#' @return
#' @export
#' @import dplyr
#' @import logger
#'
#' @examples
proc_batch_nts <- function(escon, rfindex, esids, tempsavedir, ingestpth, configfile, 
                            coresBatch, noIngest = FALSE) {
  # These are spec sources which pollute the comp_group field and need to be removed
  # TODO change structure of CSL so that data sources are not mixed with compound groups
  SPEC_SOURCES <- c("BfG", "LfU", "UBA")
  # These fields are copied from msrawfiles to the results
  FIELDS_MERGE <- c(
    "filename", 
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
    "comment"
  )
  bStartTime <- lubridate::now()
  
  # Define internal functions ####
  gfield <- get_field_builder(escon = escon, index = rfindex)
  
  proc_esid_pp <- function(escon, rfindex, esid) {  # esid <- esids[1]
    gf <- get_field_builder(escon = escon, index = rfindex)
    dataPath <- gf(esid, "path")
    rawFile <- suppressMessages(xcms::xcmsRaw(dataPath, includeMSn = TRUE))
    
    plTemp <- ntsworkflow::FindPeaks_BfG(
      rawFile, 
      gf(esid, "nts_mz_min"), 
      gf(esid, "nts_mz_max"), 
      gf(esid, "nts_mz_step"),
      gf(esid, "nts_rt_min") * 60,
      gf(esid, "nts_rt_max") * 60,
      gf(esid, "nts_sn"),
      gf(esid, "nts_int_threshold"),
      gf(esid, "nts_peak_noise_scans"),
      gf(esid, "nts_precursor_mz_tol"),
      gf(esid, "nts_peak_width_min"),
      gf(esid, "nts_peak_width_max"),
      gf(esid, "nts_max_num_peaks")
    )
    
    plTemp2 <- ntsworkflow::componentization_BfG(
      plTemp, 
      daten = rawFile, 
      ppm = gf(esid, "nts_componentization_ppm"), 
      Grenzwert_RT = gf(esid, "nts_componentization_rt_tol"), 
      Grenzwert_FWHM_left = gf(esid, "nts_componentization_rt_tol_l"), 
      Grenzwert_FWHM_right = gf(esid, "nts_componentization_rt_tol_r"), 
      Summe_all = gf(esid, "nts_componentization_rt_tol_sum"),
      adjust_tolerance = gf(esid, "nts_componentization_dynamic_tolerance")
    )
    
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
      gf(esid, "nts_mz_min"), 
      gf(esid, "nts_mz_max")
    )
    settings$mz_step <- gf(esid, "nts_mz_step")
    settings$rtrange <- c(
      gf(esid, "nts_rt_min") * 60, 
      gf(esid, "nts_rt_max") * 60)
    settings$peakwidth <- c(
      gf(esid, "nts_peak_width_min"),
      gf(esid, "nts_peak_width_max")
    )
    
    settings$NoiseScans <- gf(esid, "nts_peak_noise_scans")
    settings$sn <- gf(esid, "nts_sn")
    settings$int_threshold <- gf(esid, "nts_int_threshold")
    settings$precursormzTol <- gf(esid, "nts_precursor_mz_tol")
    settings$ppm <- gf(esid, "nts_componentization_ppm")
    settings$RT_Tol <- gf(esid, "nts_componentization_rt_tol")
    settings$PPTableRow <- 1  # I changed this to 1, the whole thing makes no sense to me
    settings$orderType <- list(list(0,"asc"))  # what is the point?
    settings$TableLength <- 10  # what is the point?
    settings$displayStart <- 0  # what is the point?
    settings$maxNumPeaks <- gf(esid, "nts_max_num_peaks")
    
    logger::log_info("Completed pp on file {basename(dataPath)}")
    
    list(pl = plTemp2, rf = rawFile, set = settings)
  }
  
  # Define variables ####
  
  is <- gfield(esids, "dbas_is_name", justone = T)
  eicExtraction <- gfield(esids, "nts_eic_extraction_width", justone = T) / 1000
  thispol <- gfield(esids, "pol", justone = T)
  
  # For saving RDS file
  savename <- gsub("/", "_", dirname(gfield(esids, "path"))[1])
  savename <- gsub("\\.", "_", savename)
  # Take everything after messdaten...
  savename <- stringr::str_match(savename, "messdaten(.*)$")[,2]
  savename <- paste0("nts-batch--", savename, "--", format(Sys.time(), "%y%m%d"), ".RDS")
  savename <- file.path(tempsavedir, savename)
  
  # Use all spec sources for annotate_grouped
  dbPath <- gfield(esids, "dbas_spectral_library", justone = T)
  sdb <- con_sqlite(dbPath)
  specSource <- tbl(sdb, "experimentGroup") %>% 
    select(name) %>% distinct() %>% collect() %>% .$name
  DBI::dbDisconnect(sdb)
  # TODO change annotate_grouped so that the instruments and sources do not need
  # to be explicitly passed. Just have NULL or all which will include all of them.
  
  # Start processing ####
  # Create sampleList
  sampleList <- data.frame(
    ID = seq_len(length(esids)),
    File = gfield(esids, "path"),
    sampleType = ifelse(gfield(esids, "blank"), "Blank", "Unknown"),
    optMzStep = gfield(esids, "nts_mz_step"),
    RAM = FALSE,
    deleted = FALSE
  )
  
  log_info("Starting peak-picking and componentization")
  
  # Peak-picking ####
  
  protopeaklist <- parallel::mclapply(
    esids, 
    proc_esid_pp, 
    escon = escon,
    rfindex = rfindex,
    mc.preschedule = FALSE,
    mc.cores = coresBatch
  )
  
  # Check results
  if (!all(vapply(protopeaklist, function(x) all(c("pl", "rf", "set") %in% names(x)), logical(1))))
    stop("Peak picking unsuccessful")
  
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
    gfield(esids, "nts_alig_delta_mz", justone = T), 
    gfield(esids, "nts_alig_delta_rt", justone = T), 
    gfield(esids, "nts_alig_mz_tol_units", justone = T)
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
  log_info("Second stage componentization on batch size {length(esids)}")
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
  ftype <- gfield(esids, "nts_alig_filter_type", justone = T)
  log_info("Alignment table filter of type {ftype}")
  switch(
    ftype,
    consecutive = {
      grouped <- ntsworkflow::keepConsecutive(
        alignment = grouped, 
        samples = sampleList[sampleList$sampleType == "Unknown", "ID"],
        consecutive = gfield(esids, "nts_alig_filter_num_consecutive", justone = T)  
      )
    },
    replicate = {
      grouped <- ntsworkflow::keepReps(
        alignment = grouped, 
        samples = sampleList[sampleList$sampleType == "Unknown", "ID"], 
        reps = gfield(esids, "nts_alig_filter_num_replicates", justone = T),
        least = gfield(esids, "nts_alig_filter_min_features", justone = T)
      )
    },
    min_features = {
      # removeRare uses all samples (including blanks)
      grouped <- ntsworkflow::removeRare(
        grouped, 
        gfield(esids, "nts_alig_filter_min_features", justone = T)
      )
    },
    {
      log_error("nts_alig_filter_type must be one of consecutive, replicate, min_features")
      stop()
    }
  )
  
  aligRowsAfterFilter <- nrow(grouped)
  
  # Annotation ####
  log_info("Starting annotation")
  chromMethod <- ifelse(
    gfield(esids, "chrom_method", justone = T) == "bfg_nts_rp1",
    "dx.doi.org/10.1016/j.chroma.2015.11.014",
    gfield(esids, "chrom_method", justone = T)
  ) 
  
  # TODO change ntsworkflow: instrument: default settings should be to include everything
  annotationTable <- ntsworkflow::annotate_grouped(  
    sampleListLocal = sampleList,
    peakListList = peaklist,
    alignmentTable = grouped,
    db_path = gfield(esids, "dbas_spectral_library", justone = T),  
    threshold_score = gfield(esids, "nts_annotation_threshold_dp_score", justone = T),
    mztolu = gfield(esids, "nts_annotation_mz_tol", justone = T) / 1000,
    rttol = gfield(esids, "nts_annotation_rt_tol", justone = T),
    polarity = gfield(esids, "pol", justone = T),
    CE = c(
      gfield(esids, "nts_annotation_ce_min", justone = T),
      gfield(esids, "nts_annotation_ce_max", justone = T)
    ),
    CES = c(
      gfield(esids, "nts_annotation_ces_min", justone = T),
      gfield(esids, "nts_annotation_ces_max", justone = T)
    ),
    instrument = unlist(gfield(esids, "dbas_instr")[,1]),
    mztolu_ms2 = gfield(esids, "nts_annotation_ms2_mz_tol", justone = T) / 1000,
    rtoffset = gfield(esids, "nts_annotation_rt_offset", justone = T),
    intCutData = gfield(esids, "nts_annotation_int_cutoff", justone = T),
    numcores = 1,
    datenListLocal = datenList,
    chrom_method = chromMethod,
    expGroups = specSource
  ) 
  
  log_info("Completed annotation")
  
  stopifnot(!is.null(annotationTable))
  # Set IS peaks ####
  log_info("Setting IS peaks")
  aligIdIs <- subset(annotationTable, name == is, alignmentID, drop = TRUE)
  if (length(aligIdIs) > 1) {
    # keep only highest row
    intCols <- grouped[grouped[, "alignmentID"] %in% aligIdIs, 
                       c(grep("Int_", colnames(grouped)), grep("alignmentID", colnames(grouped)))]
    intCols <- as.data.frame(intCols)
    intCols$sum <- apply(intCols[, grep("Int_", colnames(intCols))], 1, sum)
    aligIdIs <- intCols[which.max(intCols$sum), "alignmentID"]
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
  grouped <- ntsworkflow::blankCorrection(
    alignment = grouped, 
    samplels = sampleList, 
    intensityFactor = gfield(esids, "nts_blank_correction_factor", justone = T),  
    deleteGrouped = FALSE  # This needs to be tested if it really works before setting it to true
  )
  
  log_info("Completed blank correction")
  
  # clear the anntation table for rows which are no longer in the alignment table
  # after blank correction
  annotationTable <- subset(annotationTable, alignmentID %in% grouped[, "alignmentID"])
  
  # Save RDS file ####
  
  allData <- list(
    peaklist = peaklist, 
    peakPickSettings = peakPickSettings, 
    datenList = datenList, 
    sampleList = sampleList,
    grouped = grouped,
    annotationTable = annotationTable
  )
  
  saveRDS(allData, savename)
  rm(allData)
  # Print some statistics
  log_info("Saved RDS file")
  message("--- Stats ---")
  message("\nNumber of samples processed: ", length(esids))
  message("Alignment features uncorrected: ", aligRowsUncorrected)
  message("Correction by filter ", round(100*(1-(aligRowsAfterFilter/aligRowsUncorrected))), "%")
  message("Correction by blanks ", round(100*nrow(grouped)/rowsBeforeBlank), "%")
  message("Annotated features: ", round(100*nrow(annotationTable)/nrow(grouped)), "%")
  message("------------")
  
  # Create JSON ####
  
  # Clean alignment table ####
  alig <- grouped
  alig <- as.data.frame(alig)
  annot <- annotationTable
  samp <- sampleList
  pll <- peaklist
  pl <- do.call("rbind", pll)
  rm(grouped, annotationTable, sampleList, peaklist, pll)
  
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
  alig2$filename <- vapply(
    alig2$sample, 
    function(x) basename(samp[samp$ID == x, "File"]), 
    character(1)
  )
  alig2 <- alig2 %>% 
    rename(rt = RT, intensity_from_alig = Int, cas = CAS, smiles = SMILES)
  alig2$area <- vapply(
    alig2$PeakID, 
    function(x) pl[pl$peak_id_all == x, "Area"], 
    numeric(1)
  )
  alig2$intensity <- vapply(
    alig2$PeakID, 
    function(x) pl[pl$peak_id_all == x, "Intensity"], 
    numeric(1)
  )
  
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
    name_is = is
  )
  # Fill NAs with averages
  if (any(is.na(is_alig$intensity_is)) || any(is.na(is_alig$area_is))) {
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
  fpRow <- alig3$name %in% unique(unlist(gfield(esids, "dbas_fp")))
  alig3[fpRow, c("name", "cas", "formula", "smiles", "adduct")] <- NA
  
  # Add component information
  numGr <- table(alig3$Gruppe, alig3$sample)
  batchId <- round(as.numeric(Sys.time()))  # use unix time as id for batch
  alig3$component <- vapply(seq_len(nrow(alig3)), function(i) {
    gr <- alig3[i, "Gruppe"]
    sp <- alig3[i, "sample"]
    if (gr == 0 || numGr[as.character(gr), as.character(sp)] <= 1) {
      return(NA)
    } else {
      return(as.numeric(paste0(batchId, gr)))
    }
  }, numeric(1))
  
  # Compute normalized intensity and area ####
  if (all(c("intensity", "intensity_is") %in% colnames(alig3)) && all(alig3$intensity_is != 0))
    alig3$intensity_normalized <- alig3$intensity / alig3$intensity_is
  if (all(c("area", "area_is") %in% colnames(alig3)) && all(alig3$area_is != 0))
    alig3$area_normalized <- alig3$area / alig3$area_is
  
  alig3$intensity_from_alig <- NULL
  alig3$sample <- as.numeric(alig3$sample)
  
  # Isotopologue
  # Assume that for one compound, these have distinct nominal masses.
  # Use the name, nominal mass and adduct to determine the isotope.
  # TODO this should be incorporated into ntsworkflow
  alig3$nom_mass <- round(alig3$mz)
  
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
  # make combined docs
  combinedDocs <- lapply(dupPeakIds, function(x) {  #  x <- dupPeakIds[1]
    toCombi <- alig4[vapply(alig4, function(doc) doc$PeakID == x, logical(1))]
    newDoc <- toCombi[[1]]
    if (!is.na(newDoc$name)) {
      newDoc[["name"]] <- vapply(toCombi, function(doc) doc$name, character(1))
      newDoc[["cas"]] <- vapply(toCombi, function(doc) doc$cas, character(1))
      newDoc[["formula"]] <- vapply(toCombi, function(doc) doc$formula, character(1))
      newDoc[["smiles"]] <- vapply(toCombi, function(doc) doc$smiles, character(1))
      newDoc[["adduct"]] <- vapply(toCombi, function(doc) doc$adduct, character(1))
      newDoc[["inchi"]] <- vapply(toCombi, function(doc) doc$inchi, character(1))
      newDoc[["inchikey"]] <- vapply(toCombi, function(doc) doc$inchikey, character(1))
      cg <- unique(unlist(lapply(toCombi, function(doc) doc$comp_group)))
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
  
  res2 <- elastic::docs_mget(escon, rfindex, ids = esids, source = FIELDS_MERGE,
                             verbose = F)
  sdata <- lapply(res2$docs, function(x) x[["_source"]])
  # Attach sample information ####
  alig6 <- lapply(alig6, function(doc) { # doc <- alig6[[1]]
    sampdoc <- sdata[[doc$sample]]
    c(doc, sampdoc)
  })
  
  # Clean up environment ####
  rm(list = ls()[!is.element(ls(), c(
    "alig6", "alig3b", "pl", "savename", "gfield", "coresBatch", "samp", "rfindex", "eicExtraction"))])
  
  # Collect EIC, MS1 and MS2 ####
  bySamp <- split(alig6, sapply(alig6, function(doc) doc$sample))
  bySamp2 <- parallel::mclapply(bySamp, function(aligSamp) {  # aligSamp <- bySamp[[1]]
    stopifnot(length(unique(sapply(aligSamp, function(doc) doc$filename))) == 1)
    doc1 <- aligSamp[[1]]
    filePath <- samp[samp$ID == doc1$sample, "File", drop = T]
    stopifnot(is.character(filePath), file.exists(filePath))
    rf <- suppressMessages(xcms::xcmsRaw(filePath, includeMSn = T))
    aligSampNew <- lapply(aligSamp, function(doc) {  # doc <- aligSamp[[1]]
      # Collect eic
      lert <- pl[pl$peak_id_all == doc$PeakID, "LeftendRT"] - 20
      rert <- pl[pl$peak_id_all == doc$PeakID, "RightendRT"] + 20
      eic <- xcms::rawEIC(
        rf,
        mzrange = c(
          doc$mz - eicExtraction / 2, 
          doc$mz + eicExtraction / 2
        ),
        rtrange = c(lert, rert)
      )
      eic$time <- round(rf@scantime[eic$scan], 4)  # eic.time is in seconds!
      # reduce number of rows by calculating average intensities
      # want to have about 50 data points per peak
      reduction <- floor(length(eic$time) / 50)
      if (reduction < 1)  # if this is the case no need for reduction
        reduction <- 1
      oo <- zoo::zoo(cbind(eic$time, eic$int), eic$scan)
      oor <- zoo::rollapply(oo, reduction, mean, by = reduction)
      eicdf <- data.frame(time = oor[,1], int = oor[,2])
      # make sure there are no values below 0
      eicdf <- eicdf[eicdf$int > 0, ]
      eicl <- split(eicdf, seq_len(nrow(eicdf)))
      eicl <- lapply(eicl, as.list)
      doc[["eic"]] <- unname(eicl)
      
      # Collect MS1
      ms1scan <- pl[pl$peak_id_all == doc$PeakID, "Scan", drop = T][1]
      stopifnot(is.numeric(ms1scan), ms1scan != 0)
      ms1 <- as.data.frame(xcms::getScan(rf, ms1scan))
      stopifnot(nrow(ms1) > 0)
      colnames(ms1) <- c("mz", "int")

      # Focus only on area of spec around mz of feature
      stopifnot(length(doc$mz) == 1, is.numeric(doc$mz))
      ms1 <- ms1[ms1$mz > doc$mz - 1 & ms1$mz < doc$mz + 40, , drop = F]

      # Remove noise
      # 1) remove anything below baseline intensity
      bl <- pl[pl$peak_id_all == doc$PeakID, "Baseline", drop = T][1]
      stopifnot(is.numeric(bl))
      ms1 <- ms1[ms1$int >= bl, , drop = F]
      # 2) remove anything in the "noise region" where you cant distinguish anything anyway
      # look for the intensity level where the maximum diff between mz is less than one
      # at this level, there are peaks everywhere, so we can remove them
      # segm <- seq(0, max(ms1$int), length.out = 20)
      # getDiff <- function(x) {
      #   subms <- ms1[ms1$int >= x, "mz"]
      #   if (length(subms) < 2)
      #     return(NA)
      #   max(diff(subms))
      # }
      # gaps <- data.frame(breaks = segm, max_gap = sapply(segm, getDiff))
      # newNoise <- max(subset(gaps, max_gap < 1, breaks))
      # ms1 <- ms1[ms1$int >= newNoise, , drop = F]

      # if ("ms1" %in% names(doc)) {
      #   oldMz <- vapply(doc$ms1, function(x) x$mz, numeric(1))
      #   known <- vapply(oldMz, function(x) any(abs(ms1$mz - x) <= se$mztol),
      #                  logical(1))
      #   oldMs1 <- doc$ms1[!known]
      #   oldMs1 <- do.call("rbind", lapply(oldMs1, as.data.frame))
      #   ms1 <- rbind(ms1, oldMs1)
      # }
      stopifnot(nrow(ms1) > 0)
      ms1 <- split(ms1, seq_len(nrow(ms1)))
      ms1 <- lapply(ms1, as.list)
      doc[["ms1"]] <- unname(ms1)
      
      # collect MS2 ####
      ms2Scan <- alig3b[alig3b$PeakID == doc$PeakID, "ms2scan", drop = T][1]
      stopifnot(is.numeric(ms2Scan))
      if (ms2Scan == 0)
        return(doc)
      
      ms2 <- as.data.frame(xcms::getMsnScan(rf, ms2Scan))
      
      stopifnot(nrow(ms2) > 0)
      colnames(ms2) <- c("mz", "int")
      
      # cut off anything above mz of precursor
      ms2 <- ms2[ms2$mz < doc$mz + 1, ]
      # in case there are a lot of fragments, cut out bottom 2% of intensities
      if (nrow(ms2) > 20) {
        noiseMS2 <- max(ms2$int)*0.02
        ms2 <- ms2[ms2$int > noiseMS2, ]
      }
      # ggplot(ms2, aes(mz, 0, xend = mz, yend = int)) + geom_segment()
      
      ms2 <- split(ms2, seq_len(nrow(ms2)))
      ms2 <- lapply(ms2, as.list)
      doc[["ms2"]] <- unname(ms2)
      doc
    })
    # Reduce size of rawFile
    rf@env$intensity <- NULL
    rf@env$mz <- NULL
    rf@env$profile <- NULL
    rf@env$msnIntensity <- NULL
    rf@env$msnMz <- NULL
    aligSampNew
  }, mc.preschedule = FALSE, mc.cores = coresBatch)
  alig7 <- do.call("c", bySamp2)
  
  
  alig7 <- lapply(alig7, function(doc) doc[names(which(!is.na(doc)))])
  
  # remove fields
  alig7 <- lapply(alig7, function(doc) {
    doc$sample <- NULL
    doc$PeakID <- NULL
    doc$ms2scan <- NULL
    doc
  })
  alig7 <- unname(alig7)
  jsonPath <- sub("\\.RDS$", ".json", savename)
  
  # Write JSON ####
  log_info("Writing JSON file {dataPath2}")
  jsonlite::write_json(alig7, dataPath2, auto_unbox = T, pretty = T)
  
  
  
  if (!noIngest) {
    bindex <- gfield(esids, "dbas_index_name", justone = T)
    log_info("Ingest starting")
    
    system(
      glue::glue("{ingestpth} {configfile} {bindex} {jsonPath} &> /dev/null")
    )
    
    log_info("Ingest step finished, checking db")
    
    # Need to add a pause so that elastic returns ingested docs
    Sys.sleep(10)
    # Check that everything is in the database
    checkFiles <- gfield(esids, "filename")
    
    resp5 <- elastic::Search(
      escon, bindex, size = 0, 
      body = list(query = list(terms = list(filename = checkFiles)))
    )
    
    if (resp5$hits$total$value == length(alig7)) {
      log_info("All docs imported into ElasticSearch")
      # Add processing date and spectral library checksum to msrawfiles
      for (i in esids) {
        tryCatch(
          add_latest_eval(escon, rfindex, esid = i, fieldName = "nts_last_eval"),
          error = function(cnd) {
            log_error("Could not add processing time to id {i}")
            message(cnd)
          }
        )
        tryCatch(
          add_sha256_spectral_library(escon, rfindex, esid = i),
          error = function(cnd) {
            log_error("Could not add sha256 of spec lib to id {i}")
            message(cnd)
          }
        )
      }
    } else {
      #browser()
      log_warn("Ingested data not found in batch starting with id {esids[1]}")
    }
  }
  
  log_info("Compressing json with gzip")
  system2("gzip", jsonPath)
  
  # Add processingtime information to msrawfiles after all steps.
  bEndTime <- lubridate::now()
  mins <- round(as.numeric(bEndTime - bStartTime, units = "mins"))
  res3 <- es_add_field(escon, rfindex, "nts_proc_time", 
               queryBody = list(ids = list(values = esids)), value = mins)
  
}