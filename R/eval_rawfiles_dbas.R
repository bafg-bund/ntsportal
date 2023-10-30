
# Functions for eval-rawfiles-dbas.R

# esids <- allFlsIds[[78]]
# cat(paste(shQuote(esids, type = "cmd"), collapse = ", "))
# rfindex <- RFINDEX
# fieldName <- "duration"


#' Check that fields are the same for a set of documents
#'
#' @param escon 
#' @param rfindex 
#' @param esids 
#' @param fieldName 
#' @param onlyNonBlanks 
#'
#' @return
#' @export
#'
check_uniformity <- function(escon, rfindex, esids, fieldName, onlyNonBlanks = F) {
  bodyAll <- sprintf(
    '
      {
        "query": {
          "terms": {
            "_id": [%s]
          }
        },
        "size": 0,
        "aggs": {
          "uniformity": {
            "cardinality": {
              "field": "%s"
            }
          }
        }
      }
      ',paste(shQuote(esids, type = "cmd"), collapse = ", "), fieldName)
  
  bodyNonBlanks <- sprintf(
    '
      {
        "query": {
          "bool": {
            "must": [
              {
                "terms": {
                  "_id": [%s]
                }
              },
              {
                "term": {
                  "blank": {
                    "value": false
                  }
                }
              }
            ]
          }
        },
        "size": 0,
        "aggs": {
          "uniformity": {
            "cardinality": {
              "field": "%s"
            }
          }
        }
      }
      ',paste(shQuote(esids, type = "cmd"), collapse = ", "), fieldName)
  
  
  res <- elastic::Search(
    escon, rfindex, 
    body = ifelse(onlyNonBlanks, bodyNonBlanks, bodyAll)
  )
  if (res$aggregations$uniformity$value == 1 ||
      res$aggregations$uniformity$value == 0) {
    return(TRUE) 
  } else {
    warning("Non-uniformity in batch ", esids[1], " and field ", fieldName)
    return(FALSE)
  } 
}


#' Get a field for a set of documents
#'
#' @param escon Elasticsearch connection object
#' @param indexName 
#' @param esids Document IDs in the named index
#' @param fieldName field to extract
#' @param simplify 
#' @param justone 
#'
#' @return
#' @export
#'
get_field <- function(escon, indexName, esids, fieldName, simplify = T, justone = F) {
  if (length(esids) == 1) {
    res <- elastic::docs_get(
      escon, indexName, id = esids, source = fieldName, verbose = F
    )  
    ret <- res[["_source"]][[fieldName]]
  } else {
    res <- elastic::docs_mget(
      escon, indexName, ids = esids, source = fieldName, verbose = F
    )
    if (simplify) {
      ret <- sapply(res$docs, function(x) {
        temp <- x[["_source"]][[fieldName]]
        if (is.null(temp))
          NA else temp
      })
    } else {
      ret <- lapply(res$docs, function(x) x[["_source"]][[fieldName]])
    }
  }
  
  if (justone) {
    #browser(expr = esids[1] == "A4RukokBcjCrX8D7Tn6D" && fieldName == "duration")
    ret <- Filter(Negate(is.null), ret)
    ret <- Filter(Negate(is.na), ret)
    ret <- unique(ret)
    if (length(ret) > 1)
      stop("There is more than one type of ", fieldName, " in batch starting with ", esids[1])
    ## TODO this is causing a problem!!!
    if (length(ret) == 0)
      stop("There is no ", fieldName, " in batch starting with ", esids[1])
  }
  ret
}


#' Get rare compounds from a Report
#' 
#' Get the compounds which are found fewer than min_freq
#' 
#' @param repo 
#' @param min_freq 
#'
#' @return
#'
get_rare <- function(repo, min_freq) {
  pl <- repo$peakList
  finds <- by(pl, pl$comp_name, nrow)
  rare <- which(finds < min_freq)
  names(rare)
}

#' Normalize ms2 spectrum to the maximum intensity
#'
#' @param x 
#' @param precursormz 
#' @param mztol 
#' @param noiselevel 
#'
#' @return
#'
norm_ms2 <- function(x, precursormz, mztol = 0.02, noiselevel = 0.1) {
  # remove precursor and noise
  x <- x[x$mz < (precursormz - mztol), ]  # remove precursor otherwise MS1 checked again
  x <- x[x$int >= noiselevel, ]
  if (length(x) == 0 || nrow(x) == 0)
    return(NULL)
  data.frame(mz = x$mz, int = x$int / max(x$int))
}

#' Normalize ms1 spectrum to the maximum intensity
#'
#' @param x 
#' @param precursormz 
#' @param precursorInt 
#' @param noiselevel 
#'
#' @return
norm_ms1 <- function(x, precursormz, precursorInt, noiselevel = 0.1) {
  # remove noise
  x <- x[x$int >= noiselevel, ]
  if (length(x) == 0 || nrow(x) == 0)
    return(NULL)
  data.frame(mz = x$mz, int = x$int / precursorInt)
}



#' Remove fields in a doc that are or contain NAs
#'
#' @param doc 
#'
#' @return list document with NAs removed
#' @export
#'
remove_na_doc <- function(doc) {
  doc <- lapply(doc, function(field) {
    if (is.vector(field)) {
      field <- field[!is.na(field)]
      field <- field[!(field == "NA")]
      if (length(field) == 0)
        return(NULL) else return(field)
    } else if (is.data.frame(field)) {
      keep <- apply(field, 1, function(fieldEntry) {
        !any(is.na(fieldEntry) | fieldEntry == "NA") 
      })
      field <- field[keep, ]
      if (nrow(field) == 0)
        return(NULL) else return(field)
    } else if (is.list(field)) {
      field <- lapply(field, function(fieldEntry) {
        fieldEntry <- fieldEntry[!any(is.na(fieldEntry))]
        fieldEntry <- fieldEntry[!any(fieldEntry == "NA")]
        fieldEntry
      })
      if (length(field) == 0)
        return(NULL) else return(field)
    } else {
      stop("unknown case when removing NAs")
    }
  })
  Filter(Negate(is.null), doc)
}

#' Check that a field exists in all documents in rawfiles index
#'
#' @param escon 
#' @param rfindex 
#' @param fieldName 
#' @param onlyNonBlank logical, default FALSE, if TRUE, blanks will be ignored
#'
#' @return TRUE if all docs have the field
#' @export
#'
check_field <- function(escon, rfindex, fieldName, onlyNonBlank = FALSE) {
  res <- if (onlyNonBlank) {
    elastic::Search(escon, rfindex, body = sprintf('
    {
      "query": {
        "bool": {
          "must_not": [
            {
              "exists": {
                "field": "%s"
              }
            }
          ],
          "must": [
            {
              "term": {
                "blank": {
                  "value": false
                }
              }
            }
          ]
        }
      }
    }
                      
    ', fieldName))
  } else {
    elastic::Search(escon, rfindex, body = sprintf('
    {
      "query": {
        "bool": {
          "must_not": [
            {
              "exists": {
                "field": "%s"
              }
            }
          ]
        }
      }
    }
                      
    ', fieldName))
  }
   
  res$hits$total$value == 0
}





# Processing functions ####

# will go through all stages dbas1-3+ingest for all files in the 
# batch

# 
#' Process an MS measurement file
#' 
#' Takes an esid from the rawfiles index and carries out dbas processing for that file
#' 
#' @param escon 
#' @param rfindex 
#' @param esid 
#'
#' @return an object of class ntsworkflow::Report
#' @export
#' @import ntsworkflow
proc_esid <- function(escon, rfindex, esid, compsProcess = NULL) { 
  #browser()
  stopifnot(length(esid) == 1)
  dc <- elastic::docs_get(escon, rfindex, esid, verbose = F)
  dc <- dc$`_source`
  
  dbas <- Report$new()
  dbas$addRawFiles(F, dc$path)
  dbas$addIS(F, dc$dbas_is_table)
  dbas$addDB(F, dc$dbas_spectral_library)
  dbas$changeSettings("blank_int_factor", dc$dbas_blank_int_factor)
  dbas$changeSettings("area_threshold", dc$dbas_area_threshold)
  dbas$changeSettings("rttolm", dc$dbas_rttolm)
  dbas$changeSettings("mztolu", dc$dbas_mztolu)
  dbas$changeSettings("mztolu_fine", dc$dbas_mztolu_fine)
  dbas$changeSettings("use_int_threshold", "area")
  dbas$changeSettings("threshold", dc$dbas_ndp_threshold)
  dbas$changeSettings("rtTolReinteg", dc$dbas_rtTolReinteg)
  dbas$changeSettings("ndp_m", dc$dbas_ndp_m)
  dbas$changeSettings("ndp_n", dc$dbas_ndp_n)
  dbas$changeSettings("instr", unlist(dc$dbas_instr))
  dbas$changeSettings("pol", dc$pol)  
  
  # for the bfg method we are still using the old naming scheme (doi as name)
  if (dc$chrom_method == "bfg_nts_rp1") {
    dbas$changeSettings("chromatography", "dx.doi.org/10.1016/j.chroma.2015.11.014")
  } else {
    dbas$changeSettings("chromatography", dc$chrom_method)
  }
  dbas$changeSettings("numcores", 1)
  crash <- FALSE
  tryCatch(
    suppressMessages(dbas$process_all(comp_names = compsProcess)), 
    error = function(cnd) {
      log_warn("Processing error in file ", dc$path)
      crash <<- TRUE
      message(cnd)
    }
  )
  if (crash)
    return(NULL)
  
  dbas$clearData()
  dbas
}


#' Process a batch of MS measurement files stored in the msrawfiles index
#' 
#' takes a series of esids and processes these as a batch. 
#' 
#' @param escon 
#' @param rfindex 
#' @param esids 
#' @param tempsavedir 
#' @param ingestpth 
#' @param configfile 
#' @param coresBatch 
#' @param noIngest logical, for testing purposes, no upload, just create json.
#'
#' @return
#' @export
#'
proc_batch <- function(escon, rfindex, esids, tempsavedir, ingestpth, configfile, 
                       coresBatch, noIngest = FALSE) {
  
  # create a get_field function with some default parameters
  get_field_builder <- function(escon, index) {
    function(esids, fieldName, simplify = T, justone = F) {
      ntsportal::get_field(escon = escon, indexName = index, esids = esids, 
                           fieldName = fieldName, simplify = simplify, justone = justone)
    }
  }
  get_field2 <- get_field_builder(escon = escon, index = rfindex)
  
  dr <- paste(unique(dirname(get_field2(esids, "path", justone = F))), collapse= ", ")
  log_info("Starting batch {dr}")
  # generate name for saving the files
  savename <- tempfile("batch-", tmpdir = tempsavedir, fileext = ".report")
  tryCatch({
    # Step 1 - Screening ####
    tryCatch(
      repLt <- parallel::mclapply(
        esids, 
        proc_esid, 
        escon = escon, 
        rfindex = rfindex,
        mc.cores = coresBatch,
        mc.preschedule = F
      ),
      error = function(cnd) {
        log_warn("Error in screening for batch with id {esid[1]}")
        message(cnd)
      }
    )
    
    #browser()
    if (any(vapply(repLt, is.null, logical(1))) || 
        any(vapply(repLt, inherits, logical(1), what = "try-error"))) {
      errorEsids <- c(
        esids[vapply(repLt, is.null, logical(1))],
        esids[vapply(repLt, inherits, logical(1), what = "try-error")]
      )
      errorFiles <- allFls[allFls$id %in% errorEsids, "path"]
      errorFilesClps <- paste(errorFiles, collapse = "\n")
      log_warn("Error in files {errorFilesClps}")
    }
    # remove errors
    repLt <- Filter(Negate(is.null), repLt)
    repLt <- Filter(function(x) Negate(inherits)(x, what = "try-error"), repLt)
    
    # remove reports with no peaks
    nothingFound <- vapply(repLt, function(x) nrow(x$peakList) == 0, logical(1))
    
    if (any(nothingFound)) {
      nothingEsids <- esids[nothingFound]
      nothingFiles <- allFls[allFls$id %in% nothingEsids, "path"]
      message(sprintf("\nFiles\n%s\nhave no hits.", 
                      paste(nothingFiles, collapse = "\n")))
    }
    
    repLt <- Filter(function(x) nrow(x$peakList) != 0, repLt)  
    
    log_info("Merging reports")
    if (length(repLt) == 1) {
      resM <- repLt[[1]]
    } else if (length(repLt) > 1) {
      resM <- Reduce(ntsworkflow::mergeReport, repLt)
    } else {
      warning("\nNo report files were generated")
      return(NULL)
    }
    
    log_info("Saving report file at {savename}")
    resM$clearAndSave(F, savename)
    numPeaks <- nrow(resM$peakList)
    rm(repLt, resM)
    log_info("Completed step 1 with {numPeaks} peaks")
    if (!file.exists(savename))
      return(NULL)
    
    # Step 2 - Correction and reintegration ####
    
    # The settings used must be the same for all the files in the batch
    # need to check that all entries in batch have the same
    
    # blank_regex settings
    bregex <- get_field2(esids, "dbas_blank_regex")
    stopifnot(is.vector(bregex))
    bregex <- unique(bregex)
    stopifnot(length(bregex) == 1)
    
    # load
    dbas <- loadReport(F, savename)
    dbas$changeSettings("numcores", 1)
    
    log_info("Blank correction")
    
    if (!is.null(bregex) && any(grepl(bregex, dbas$rawFiles))) {
      dbas$deleteBackground(grep(bregex, dbas$rawFiles, invert = T), 
                            grep(bregex, dbas$rawFiles))
      dbas$remRawFiles(grep(bregex, dbas$rawFiles))
      esids <- esids[!get_field2(esids, "blank", simplify = T)]
    } else {
      log_info("No blanks found in {dr}")
    }
    
    log_info("Remove false positives")
    
    # get list of false positives (will just add all of the fps)
    
    bfps <- get_field2(esids, "dbas_fp", simplify = F)
    # TODO in the future, delete fps on a by-sample basis
    # for now, just add them all together into a big list
    bfps <- unique(unlist(bfps))
    
    bmindet <- unique(get_field2(esids, "dbas_minimum_detections"))
    stopifnot(length(bmindet) == 1, is.integer(bmindet))
    
    if (bmindet > 1)
      bfps <- append(bfps, get_rare(dbas, bmindet))
    
    
    if (length(bfps) > 0)
      for (fpname in bfps) dbas$deleteFP(fpname) 
    
    # remove 'B' in peak (compounds with multiple features)
    pl <- dbas$peakList
    pl2 <- subset(pl, select = c(comp_name, peak))
    pl3 <- subset(pl2, peak != "A" )
    comp_name_u <- unique(pl3$comp_name)
    if (length(comp_name_u) > 0) {
      log_info("Delete Peak 'B' of compounds:")
      for (x in comp_name_u) 
        message(paste(x, collapse = "\n"))
    }
    dbas$peakList <- dbas$peakList[dbas$peakList$peak == "A" , ]
    
    log_info("Reintegration")
    # at this stage you could increase the cores of the report so that
    # the reintegration is faster
    dbas$changeSettings("numcores", 6)
    tryCatch(
      suppressMessages(dbas$reIntegrate()),
      error = function(cnd) {
        log_warn("Error in reintegration of batch with id {esids[1]}")
      }
    )
    
    
    # save report
    newsavename <- sub("\\.report$", "_i.report", savename)
    dbas$clearAndSave(F, newsavename)
    rm(dbas)
    log_info("Step 2 complete, processed report file created for: {newsavename}")
   
    dbas <- loadReport(F, newsavename)  
    # Step 3 - Conversion ####
    log_info("Collecting data for json export")
    
    compData <- dbas$integRes[, c("samp", "comp_name", "int_a")]
    compData$samp <- basename(compData$samp)
    
    
    
    bist <- unique(get_field2(esids, "dbas_is_table"))
    stopifnot(length(bist) == 1)
    
    bisn <- unique(get_field2(esids, "dbas_is_name"))
    stopifnot(length(bisn) == 1)
    
    isData <- dbas$ISresults[dbas$ISresults$IS == bisn, c("samp", "int_a")]
    
    # normalize intensities
    dat <- merge(compData, isData, by = "samp", suffix = c("", "_IS"))
    # verify columns are numeric
    dat[, c("int_a", "int_a_IS")] <- lapply(dat[, c("int_a", "int_a_IS")], as.numeric)
    dat$norm_a <- round(dat$int_a / dat$int_a_IS, 4)
    dat$area_normalized <- dat$norm_a
    
    if (length(dat$int_a_IS) == 0)
      stop("IS not found in docs ", paste(esids, collapse = ", "))
    
    if (!all(is.na(dat$int_a_IS))) {
      rstdev <- round((sd(dat$int_a_IS)/mean(dat$int_a_IS)), 2)
      log_info("Die relative SD des internen Standards beträgt ", 100*rstdev, "%")
      # TODO The script must automatically decide what to do.
      if  (100*rstdev >= 10) 
        log_warn("Die Standardabweichung des internen Standards ist über Grenzwert von 10%")
    }
    
    brepr <- unlist(unique(get_field2(esids, "dbas_replicate_regex")))
    
    #bstation <- unique(get_field(esids, "station"))
    #stopifnot(length(bstation) == 1)
    # build average area from replicates
    if (!is.null(brepr) && !is.na(brepr)) {
      log_info("Building replicate averages for testing")
      # using the regex, give all replicate samples the same name (best when replicates are indicated by _1 at the end)
      dat$reps <- stringr::str_replace(dat$samp, brepr, "\\1")
      averages <- by(dat, list(dat$reps, dat$comp_name), function(part) {
        comp1 <- part$comp_name[1]
        samp1 <- part$samp[1]
        average_int_a <- mean(part$int_a, na.rm = T)
        average_norm_a <- mean(part$norm_a, na.rm = T)
        standard_deviation_norm_a <- sd(part$norm_a, na.rm = T)
        rsd_norm_a <- standard_deviation_norm_a/average_norm_a
        average_int_a_IS <- mean(part$int_a_IS, na.rm = T)
        data.frame(
          samp = samp1, comp_name = comp1, int_a = average_int_a,
          int_a_IS = average_int_a_IS, norm_a = average_norm_a, 
          sd_norm_a = standard_deviation_norm_a, rsd_norm_a = rsd_norm_a
        )
      }, simplify = F)
      
      datTemp <- do.call("rbind", averages)
      #saveRDS(datTemp, file = glue::glue("{esids[1]}_replicateRSD_{format(Sys.Date(), '%Y%m%d')}.RDS"))
      
      bba <- unique(get_field2(esids, "dbas_build_averages"))
      stopifnot(length(bba) == 1)
      if (bba) {
        log_info("Using averages for results")
        dat <- do.call("rbind", averages)
        # remove column
        dat$sd_norm_a <- NULL
        dat$rsd_norm_a <- NULL
        
      }
      
      # warning if sd for any comp is high
      if (any((100*datTemp$rsd_norm_a >= 30), na.rm = T))   {
        log_info("The SD of a detection in the replicates may be high")
        probleme <- subset(datTemp, 100*rsd_norm_a >= 30, comp_name, drop = TRUE)
        problemeProben <- subset(datTemp, 100*rsd_norm_a >= 30, samp, drop = TRUE)
        log_info("Problematic detections:")
        for (i in seq_along(probleme)) 
          message(probleme[i], " in sample ", problemeProben[i])
        log_warn("Check results")
      }
      
      dat$reps <- NULL
    }
    
    # round all int columns to 3 sig figs
    dat$int_a <- signif(dat$int_a, 3)
    dat$norm_a <- signif(dat$norm_a, 3)
    dat$area_normalized <- signif(dat$area_normalized, 3)
    dat$int_a_IS <- signif(dat$int_a_IS, 3)
    
    # get start time for sample
    idSamp <- data.frame(
      id = esids,
      samp = basename(get_field2(esids, "path")),
      start = get_field2(esids, "start")
    )
    
    dat <- merge(dat, idSamp, by = "samp", all.x = T)
    stopifnot(all(!is.na(dat$start)))
    
    # Get other data
    # CAS-RN
    cas <- dbas$peakList[, c("comp_CAS", "comp_name")]
    cas <- cas[!duplicated(cas),]
    dat <- merge(dat, cas, by = "comp_name", all.x = T)
    # Adduct
    tempPl <- dbas$peakList[, c("comp_name", "adduct")]
    tempPl <- tempPl[!duplicated(tempPl),]
    dat <- merge(dat, tempPl, by = "comp_name", all.x = TRUE)
    
    dat$date_import <- round(as.numeric(Sys.time()))  # epoch_seconds
    dat$duration <- get_field2(esids, "duration", justone = T)
    #dat$station <- bstation
    
    dat$pol <- get_field2(esids, "pol", justone = T)
    dat$matrix <- get_field2(esids, "matrix", justone = T)
    dat$data_source <- get_field2(esids, "data_source", justone = T)
    dat$sample_source <- get_field2(esids, "sample_source", justone = T)
    dat$licence <- get_field2(esids, "licence", justone = T)
    dat$chrom_method <- get_field2(esids, "chrom_method", justone = T)
    # change names to match ntsp
    colnames(dat) <- gsub("^comp_name$", "name", colnames(dat))
    colnames(dat) <- gsub("^int_a$", "area", colnames(dat))
    colnames(dat) <- gsub("^int_a_IS$", "area_is", colnames(dat))
    colnames(dat) <- gsub("^comp_CAS$", "cas", colnames(dat))
    colnames(dat) <- gsub("^samp$", "filename", colnames(dat))
    # make dat into list to allow for nested data structure
    rownames(dat) <- NULL
    datl <- split(dat, seq_len(nrow(dat))) 
    datl <- lapply(datl, as.list)
    
    # Get coordinates, river km
    datl <- lapply(datl, function(doc) {
      doc$loc <- get_field2(doc$id, "loc", simplify = F) 
      doc$station <- get_field2(doc$id, "station", simplify = T)
      dockm <- get_field2(doc$id, "km", simplify = T)
      if (!is.null(dockm))
        doc$km <- dockm   
      
      docriv <- get_field2(doc$id, "river", simplify = T)
      if (!is.null(docriv))
        doc$river <- docriv
      
      docgkz <- get_field2(doc$id, "glz", simplify = T)
      if (!is.null(docgkz))
        doc$gkz <- docgkz
      
      doc
    })
    
    datl <- unname(datl)
    
    log_info("Collecting spectra")
    
    datl <- lapply(datl, function(doc) {
      idtemp <- subset(dbas$peakList, comp_name == doc$name & samp == doc$filename, peakID, drop = T)
      if (!is.numeric(idtemp))
        stop("non numeric idtemp") 
      # if more than one peak matches (isomers), which should be marked by peak A, B etc, 
      # choose the peak with the highest intensity
      if (length(idtemp) > 1) { 
        best <- which.max(subset(dbas$peakList, peakID %in% idtemp, int_a, drop = T))
        idtemp <- subset(dbas$peakList, peakID %in% idtemp, peakID, drop = T)[best]
        doc$comment <- paste(doc$comment, "isomers found")
      }
      
      # mz
      mztemp <- subset(dbas$peakList, peakID == idtemp, real_mz, drop = T)
      if (length(mztemp) == 0)
        mztemp <-subset(dbas$integRes, comp_name == doc$name & samp == doc$filename, real_mz, drop = T)
      stopifnot(length(mztemp) == 1, is.numeric(mztemp), !is.na(mztemp))
      doc$mz <- round(mztemp, 4)
      
      # rt
      rttemp <- subset(dbas$peakList, peakID == idtemp, real_rt_min, drop = T)
      if (length(rttemp) == 1 && is.na(rttemp))
        rttemp <- subset(dbas$peakList, peakID == idtemp, rt_min, drop = T)
      if (length(rttemp) == 0)
        rttemp <- subset(dbas$integRes, comp_name == doc$name & samp == doc$filename, real_rt_min, drop = T)
      stopifnot(length(rttemp) == 1, is.numeric(rttemp), !is.na(rttemp))
      doc$rt <- round(rttemp, 2)
      
      # if the id was not found, then the rest of the inputs are not needed
      if (!is.numeric(idtemp) || length(idtemp) == 0)
        return(doc)
      
      # int
      inttemp <- subset(dbas$peakList, peakID == idtemp, int_h, drop = T) 
      # eic
      
      if (is.numeric(idtemp)) {
        eictemp <- subset(dbas$EIC, peakID == idtemp, c(time, int))
        if (nrow(eictemp) > 0) {
          eictemp$time <- round(eictemp$time)  # in seconds
          eictemp$int <- round(eictemp$int, 4)
          rownames(eictemp) <- NULL
          doc$eic <- eictemp
        }
        # ms1
        ms1temp <- subset(dbas$MS1, peakID == idtemp, c(mz, int))
        if (nrow(ms1temp) > 0 && is.numeric(inttemp)) {
          ms1temp <- norm_ms1(ms1temp, mztemp, inttemp)
          if (!is.null(ms1temp)) {
            ms1temp$mz <- round(ms1temp$mz, 4)
            ms1temp$int <- round(ms1temp$int, 4)
            rownames(ms1temp) <- NULL
            doc$ms1 <- ms1temp
          }
        }
        # ms2
        ms2temp <- subset(dbas$MS2, peakID == idtemp, c(mz, int))
        if (nrow(ms2temp) > 0) {
          ms2temp <- norm_ms2(ms2temp, mztemp)
          if (!is.null(ms2temp)) {
            ms2temp$mz <- round(ms2temp$mz, 4)
            ms2temp$int <- round(ms2temp$int, 4)
            rownames(ms2temp) <- NULL
            doc$ms2 <- ms2temp
          }
        }
      }
      doc
    })  
    
    # add tags if available
    datl <- lapply(datl, function(doc) {
      doctag <- get_field2(doc$id, "tag", simplify = F)
      if (!is.null(doctag))
        doc$tag <- doctag
      doc
    })
    
    # add comments if available
    datl <- lapply(datl, function(doc) {
      doccomment <- get_field2(doc$id, "comment", simplify = F)
      if (!is.null(doccomment))
        doc$comment <- doccomment
      doc
    })
    
    # remove msrawfiles id
    datl <- lapply(datl, function(doc) {doc$id <- NULL; doc})
    
    # remove NA values (there is no such thing as NA, the value just does not exist)
    datl <- lapply(datl, remove_na_doc)
    
    log_info("Writing json file")
    
    # Generate JSON ####
    jsonPath <- sub("\\.report$", ".json", newsavename)  # jsonPath <- "tests/bimmen.json"
    
    jsonlite::write_json(datl, jsonPath, pretty = T, digits = NA, auto_unbox = T)
    
    # Step 4 - Ingest ####
    if (!noIngest) {
      bindex <- get_field2(esids, "dbas_index_name", justone = T)
      log_info("Ingest starting")
      system(
        glue::glue("{ingestpth} {configfile} {bindex} {jsonPath} &> /dev/null")
      )
      log_info("Ingest complete")
      
      # need to add a pause so that elastic return ingested docs
      Sys.sleep(10)
      # check that everything is in the database
      checkFiles <- basename(get_field2(esids, "path"))
      
      resp5 <- elastic::Search(escon, bindex, body = sprintf('
    {
      "query": {
        "terms": {
          "filename": [%s]
        }
      },
      "size": 0
    }
    ', paste(dQuote(checkFiles,q = "\""), collapse = ", "))
      )
      
      if (resp5$hits$total$value == length(datl)) {
        log_info("All docs imported into ElasticSearch")
      } else {
        #browser()
        log_warn("Ingested data not found in batch starting with id {esids[1]}")
      }
      
      # Remove files
      resRemove <- file.remove(jsonPath, savename, newsavename)
      if (all(resRemove))
        log_info("Temporary files removed") else stop("Files not removed in batch", checkFiles[1])
      
      log_info("Completed batch starting with id {esids[1]}, file {checkFiles[1]}")
    }
  },
    error = function(cnd) {
      log_info("Error in proc_batch in batch starting with id {esids[1]}")
      message(cnd)
    },
    finally = system("rm -f /scratch/nts/tmp/*")
  )
  
  if (!exists("datl") || length(datl) == 0)
    return(0L)
  
  return(length(datl))
}

# IS processing functions ####

#' Process all files in msrawfiles for IS
#' 
#' Will enter the results into the isindex. Will only process files which
#' are not included in isindex.
#'
#' @param escon 
#' @param rfindex 
#' @param isindex 
#' @param ingestpth 
#' @param configfile 
#' @param tmpPath 
#' @param numCores Number of cores for parallel processing 
#'
#' @return
#' @export
#' @import dplyr
#' @import future
#' @import logger
#' 
process_is_all <- function(escon, rfindex, isindex, ingestpth, configfile, 
                           tmpPath = "/scratch/nts/tmp", numCores = 10) {
  startTime <- lubridate::now()
  # Find out what files are in msrawfiles and not in dbas_is 
  res1 <- elastic::Search(escon, isindex, body = '
    {
      "aggs": {
        "files": {
          "terms": {
            "field": "filename",
            "size": 100000
          }
        }
      },
      "size": 0
    }
  ')
  stopifnot(res1$aggregations$files$sum_other_doc_count == 0)
  filesDbasIs <- vapply(res1$aggregations$files$buckets, "[[", i = "key", character(1))
  
  res2 <- elastic::Search(escon, rfindex, body = '
    {
      "aggs": {
        "files": {
          "terms": {
            "field": "filename",
            "size": 100000
          }
        }
      },
      "size": 0
    }
  ')
  stopifnot(res2$aggregations$files$sum_other_doc_count == 0)
  filesDbasRf <- vapply(res2$aggregations$files$buckets, "[[", i = "key", character(1))
  filesToProcess <- setdiff(filesDbasRf, filesDbasIs)
  if (length(filesToProcess) == 0) {
    log_info("No files found to process")
    return(NULL)
  }
    
  # Get all ids of these files
  # There is a limit to the number of files you can look for. Set with index.maxterms
  # But it doesn't matter, this function will repeat anyway.
  if (length(filesToProcess) > 65536)
    filesToProcess <- filesToProcess[1:65536]
  
  fn_search_string <- paste(shQuote(filesToProcess, type = "cmd"), collapse = ", ")
  res3 <- elastic::Search(escon, rfindex, body = sprintf('
  {
    "query": {
      "terms": {
        "filename": [%s]
      }
    },
    "size": 10000,
    "_source": false
  }
  ', fn_search_string))
  
  log_info("Found {res3$hits$total$value} files to process")
  idsToProcess <- vapply(res3$hits$hits, "[[", i = "_id", character(1))
  plan(multicore, workers = numCores)
  featsBySample <- furrr::future_map(idsToProcess, function(id) {
    proc_is_one(escon = escon, rfindex = rfindex, esid = id)
  })
  plan(sequential)
  
  featsAll <- do.call("c", featsBySample)
  featsAll <- Filter(Negate(is.null), featsAll)
  jpth <- file.path(tmpPath, "is_dbas_temp.json")
  log_info("Writing json")
  jsonlite::write_json(featsAll, jpth, pretty = T, digits = NA, auto_unbox = T)
  log_info("Ingesting")
  system(
    glue::glue("{ingestpth} {configfile} {isindex} {jpth}")
  )
  endTime <- lubridate::now()
  hrs <- round(as.numeric(endTime - startTime, units = "hours"))
  log_info("Processing IS (process_is_all) complete in {hrs} h")
  invisible(TRUE)
}


#' Process IS in one file
#'
#' @param escon 
#' @param rfindex 
#' @param esid 
#'
#' @return
#' @export
#' @import ntsworkflow
proc_is_one <- function(escon, rfindex, esid) {
  
  # Get list of IS
  get_field_builder <- function(escon, index) {
    function(esids, fieldName, simplify = T, justone = F) {
      get_field(escon = escon, indexName = index, esids = esids, 
                fieldName = fieldName, simplify = simplify, justone = justone)
    }
  }
  get_field2 <- get_field_builder(escon = escon, index = rfindex)
  isTab <- get_field2(esid, "dbas_is_table")
  ises <- read.csv(isTab)$name
  # run dbas processing
  crash <- FALSE
  tryCatch(
    repo <- proc_esid(escon, rfindex, esid, compsProcess = ises),
    error = function(cnd) {
      log_error("IS screening id {esid[1]} failed at dbas proc_esid")
      crash <<- TRUE
      message(cnd)
    }
  )
  if (is.null(repo) || crash || nrow(repo$ISresults) == 0) {
    log_info("No results found for {esid[1]}")
    return(NULL)
  }
  crash2 <- FALSE
  tryCatch({
    isr <- repo$ISresults
    isr <- select(isr, filename = samp, name = IS, mz, rt, intensity = int_h, 
                  area = int_a, peak_start, peak_end, eic_extraction_width)
    isr$rt <- round(isr$rt, 2)
    isr$mz <- round(isr$mz, 4)
    isr$path <- get_field2(esid, "path")
    isr$start <- get_field2(esid, "start")
    isr$instrument <- get_field2(esid, "instrument")
    isr$blank <- get_field2(esid, "blank")
    isr$matrix <- get_field2(esid, "matrix")
    isr$pol <- get_field2(esid, "pol")
    isr$chrom_method <- get_field2(esid, "chrom_method")
    isr$duration <- get_field2(esid, "duration")
    isr$station <- get_field2(esid, "station")
    isr$data_source <- get_field2(esid, "data_source")
    isr$river <- get_field2(esid, "river")
    pl <- select(repo$peakList, name = comp_name, peakID)
    isr <- left_join(isr, pl, by = "name")
    rownames(isr) <- NULL
    isrl <- split(isr, seq_len(nrow(isr))) 
    isrl <- lapply(isrl, as.list)
    # add coordinates
    isrl <- lapply(isrl, function(doc) {
      doc$loc <- get_field2(esid, "loc", simplify = F) 
      doc
    })
    # add eic
    isrl <- lapply(isrl, function(doc) {
      eictemp <- subset(repo$EIC, peakID == doc$peakID, c(time, int))
      if (nrow(eictemp) > 0) {
        eictemp$time <- round(eictemp$time)  # in seconds
        eictemp$int <- round(eictemp$int, 4)
        rownames(eictemp) <- NULL
        doc$eic <- eictemp
      }
      doc
    })
    # add ms1
    isrl <- lapply(isrl, function(doc) {
      ms1temp <- subset(repo$MS1, peakID == doc$peakID, c(mz, int))
      if (nrow(ms1temp) > 0 && is.numeric(doc$intensity)) {
        ms1temp <- norm_ms1(ms1temp, doc$mz, doc$intensity)
        if (!is.null(ms1temp)) {
          ms1temp$mz <- round(ms1temp$mz, 4)
          ms1temp$int <- round(ms1temp$int, 4)
          rownames(ms1temp) <- NULL
          doc$ms1 <- ms1temp
        }
      }
      doc
    })
    # add ms2
    isrl <- lapply(isrl, function(doc) {
      ms2temp <- subset(repo$MS2, peakID == doc$peakID, c(mz, int))
      if (nrow(ms2temp) > 0) {
        ms2temp <- norm_ms2(ms2temp, doc$mz)
        if (!is.null(ms2temp)) {
          ms2temp$mz <- round(ms2temp$mz, 4)
          ms2temp$int <- round(ms2temp$int, 4)
          rownames(ms2temp) <- NULL
          doc$ms2 <- ms2temp
        }
      } 
      doc
    })
    # add tags
    isrl <- lapply(isrl, function(doc) {
      doc$tag <- get_field2(esid, "tag", simplify = F) 
      doc
    })
    # add comments
    isrl <- lapply(isrl, function(doc) {
      doc$comment <- get_field2(esid, "comment", simplify = F)
      peakWidth <- round(doc$peak_end - doc$peak_start, 2)
      doc$comment <- c(
        doc$comment, 
        paste("Peak width (min):", peakWidth),
        paste("EIC extractions width:", doc$eic_extraction_width)
      )
      doc
    })
    # Remove unneeded fields
    isrl <- lapply(isrl, function(doc) {
      doc$peak_start <- NULL
      doc$peak_end <- NULL
      doc$eic_extraction_width <- NULL
      doc$peakID <- NULL
      doc$path <- NULL
      doc$blank <- NULL
      doc
    })
    
    # Remove any NA fields
    isrl <- lapply(isrl, remove_na_doc)
    isrl <- unname(isrl)
  },
  error = function(cnd) {
    crash2 <<- TRUE
    log_error("IS screening id {esid[1]} failed at data extraction")
    message(cnd)
  })
  if (crash2) {
    return(NULL)
  }
  isrl
} 
