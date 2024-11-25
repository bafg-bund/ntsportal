

scanBatchDbas <- function(records) {
  reports <- purrr::map(records, fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  cleanedReport <- cleanReport(mergedReport, records)
  reintegratedReport <- reintegrateReport(cleanedReport)
  dbasResults <- convertToDbasResult(reintegratedReport)
  dbasResults
}


fileScanDbas <- function(msrawfileRecord, compsToProcess = NULL) {
  fileScannerDbas <- createScannerDbas(msrawfileRecord)
  fileScannerDbas <- runScanningDbas(fileScannerDbas, compsToProcess)
  fileScannerDbas
}


createScannerDbas <- function(msrawfileRecord) {
  rec <- msrawfileRecord
  scanner <- Report$new()
  scanner$addRawFiles(F, rec$path)
  scanner$addIS(F, rec$dbas_is_table)
  scanner$addDB(F, rec$dbas_spectral_library)
  scanner$changeSettings("blank_int_factor", rec$dbas_blank_int_factor)
  scanner$changeSettings("area_threshold", rec$dbas_area_threshold)
  scanner$changeSettings("rttolm", rec$dbas_rttolm)
  scanner$changeSettings("mztolu", rec$dbas_mztolu)
  scanner$changeSettings("mztolu_fine", rec$dbas_mztolu_fine)
  scanner$changeSettings("use_int_threshold", "area")
  scanner$changeSettings("threshold", rec$dbas_ndp_threshold)
  scanner$changeSettings("rtTolReinteg", rec$dbas_rtTolReinteg)
  scanner$changeSettings("ndp_m", rec$dbas_ndp_m)
  scanner$changeSettings("ndp_n", rec$dbas_ndp_n)
  scanner$changeSettings("instr", unlist(rec$dbas_instr))
  scanner$changeSettings("pol", rec$pol)
  scanner$changeSettings("numcores", 1)
  
  scanner <- changeNameChromMethod(scanner, rec)
  scanner
}

changeNameChromMethod <- function(scannerObj, msrawfileRecord) {
  # Inconsistency in naming of chromatographic method between NTSPortal and CSL
  # Corrects the naming so that it works for CSL
  if (msrawfileRecord$chrom_method == "bfg_nts_rp1") {  # new name
    scannerObj$changeSettings("chromatography", "dx.doi.org/10.1016/j.chroma.2015.11.014")  # old name
  } else {
    scannerObj$changeSettings("chromatography", msrawfileRecord$chrom_method)
  }
  scannerObj
}

runScanningDbas <- function(scannerObj, compsToProcess) {
  backupScanner <- scannerObj$copy()
  tryCatch({
    suppressMessages(scannerObj$process_all(comp_names = compsToProcess))
    scannerObj$clearData()
    warnNoPeaks(scannerObj)
    scannerObj <- placeholderToSetMockingFunctions(scannerObj)
  }, 
  error = function(cnd) {
    log_error("Processing error in file: {scannerObj$rawFiles},
                  error message: {conditionMessage(cnd)}")
  })
  if (is.null(scannerObj) || inherits(scannerObj, "try-error"))
    backupScanner else scannerObj
}

warnNoPeaks <- function(report) {
  if (nrow(report$peakList) == 0)
    log_warn("No peaks found in file: {report$rawFiles}")
}

placeholderToSetMockingFunctions <- function(x) {
  x
}


removeEmptyReports <- function(reports) {
  Filter(Negate(isEmptyReport), reports)
}

mergeReports <- function(reports) {
  if (length(reports) == 0) {
    emptyReport()
  } else if (length(reports) == 1) {
    reports[[1]]
  } else if (length(reports) > 1) {
    Reduce(ntsworkflow::mergeReport, reports)
  } 
}

cleanReport <- function(report, records) {
  if (!isEmptyReport(report)) 
    report <- blankCorrectionDbas(report, records)
  if (!isEmptyReport(report)) 
    report <- removeFalsePositives(report, records)
  if (!isEmptyReport(report)) 
    report <- removeDuplicateDetections(report)
  report
}

blankCorrectionDbas <- function(report, records) {
  blanks <- areBlanks(report, records)
  if (all(!blanks))
    return(report)
  report$deleteBackground(which(!blanks), which(blanks))
  report$remRawFiles(which(blanks))
  report
}

areBlanks <- function(report, records) {
  paths <- report$rawFiles
  isBlank <- getField(records, "blank")
  names(isBlank) <- getField(records, "path")
  isBlank[paths]
}

removeFalsePositives <- function(report, records) {
  # These fields are uniform in the records so just take the first non blank
  blanks <- getField(records, "blank")
  minimumDetections <- getField(records, "dbas_minimum_detections")[!blanks][1]
  replicateRegex <- getField(records, "dbas_replicate_regex")[!blanks][1]
  
  falsePositives <- unique(c(
    unlist(getField(records, "dbas_fp")[!blanks][1]),
    getCompoundsBelowMinimumDetections(report, minimumDetections),
    getCompoundsNoReplicateDetections(report, replicateRegex)
  ))
  
  if (length(falsePositives) > 0) {
    for (fp in falsePositives) {
      report$deleteFP(fp)
    }
  }
  report
}

removeDuplicateDetections <- function(report) {
  report$peakList <- report$peakList[report$peakList$peak == "A", ]
  report
}

getCompoundsBelowMinimumDetections <- function(report, minimumDetections) {
  if (minimumDetections > 1) {
    timesFound <- by(report$peakList, report$peakList$comp_name, nrow)
    scarce <- which(timesFound < minimumDetections)
    names(scarce)  
  } else {
    character()
  }
}

getCompoundsNoReplicateDetections <- function(report, replicateRegex) {
  if (is.na(replicateRegex)) {
    character(0)
  } else {
    pl <- report$peakList
    pl$originalSampName <- stringr::str_replace(pl$samp, replicateRegex, "\\1")
    replicateCount <- by(pl, list(pl$comp_name, pl$originalSampName), nrow)
    replicateCount <- array2DF(replicateCount)
    colnames(replicateCount) <- c("compName", "sampName", "count")
    replicateCount <- replicateCount[!is.na(replicateCount$count), ]
    
    # Take the average number of times a compound is found in each batch,
    # so if in one replicate set it is only found once, it still doesn't count as an FP
    batchCount <- data.frame(
      compName = unique(replicateCount$compName),
      count = round(tapply(replicateCount$count, replicateCount$compName, mean))
    )
    # Hard coded: Comp found at (on average) at least twice in replicates
    batchCount[batchCount$count < 2, "compName"]
  }
}

reintegrateReport <- function(report) {
  originalReport <- report$copy()
  batchPath <- dirname(report$rawFiles[1])
  tryCatch({
    suppressMessages(report$reIntegrate())
    report <- placeholderToSetMockingFunctions(report)
  },
  error = function(cnd) {
    log_warn("Error in reintegration of batch {batchPath}. Error text: {conditionMessage(cnd)}")
    report <<- originalReport
  })
  report
}

convertToDbasResult <- function(report) {
  results <- list(
    peakList = report$peakList,
    reintegrationResults = report$integRes,
    rawFilePaths = report$rawFiles,
    ms1Table = report$MS1,
    ms2Table = report$MS2,
    eicTable = report$EIC,
    isResults = report$ISresults
  )
  class(results) <- "dbasResult"
  results
}


isEmptyReport <- function(report) {
  nrow(report$peakList) == 0
}

emptyReport <- function() {
  ntsworkflow::Report$new()
}

getField <- function(listRecords, fieldName) {
  if (!fieldAvailable(listRecords, fieldName)) {
    message("Field ", fieldName, " not found in any docs")
    return(rep(NA, length(listRecords)))
  } 
  values <- lapply(listRecords, getValueOrEmpty, field = fieldName)
  sizes <- vapply(values, length, numeric(1))
  if (all(sizes == 1)) {
    unlist(values)
  } else {
    values
  }
}

fieldAvailable <- function(listRecords, fieldName) {
  any(vapply(listRecords, function(x) fieldName %in% names(x), logical(1)))
}

getValueOrEmpty <- function(record, field) {
  temp <- record[[field]]
  if (is.null(temp))
    NA else temp
}







record_end_processing <- function(escon, rfindex, idsEs, pathDb, timeStart) {
  timeText <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "GMT")
  cslHash <- system2("sha256sum", pathDb, stdout = TRUE)
  timeEnd <- lubridate::now()
  avgMins <- round(
    as.numeric(timeEnd - timeStart, units = "mins") / length(idsEs),
    digits = 2
  )
  tryCatch(
    res <- es_add_value(
      escon, rfindex, 
      queryBody = list(ids = list(values = idsEs)),
      dbas_last_eval = timeText,
      dbas_spectral_library_sha256 = cslHash,
      dbas_proc_time = avgMins
    ),
    error = function(cnd) {
      glue("Error recording processing date: {conditionMessage(cnd)}")
    }
  )
  
  if (exists("res")) {
    return(invisible(res$updated > 0))
  } else {
    return(invisible(FALSE))
  }
}

# Function to print a search query string to be used for log files
# Takes a vector of esids and a vector of fields for _source
build_es_query_for_ids <- function(ids, toShow) {
  message("\nUse the following query to search for the docs:")
  cat(
    sprintf('GET %s/_search
  {
    "query": {
      "ids": {
        "values": [%s]
      }
    },
    "_source": [%s]
  }\n', 
      rfindex, 
      paste(shQuote(ids, type = "cmd") , collapse = ", "),
      paste(shQuote(toShow, type = "cmd") , collapse = ", ")
    )
  )
  invisible(T)
}

#' Reset files in msrawfiles, so that they will be processed again.
#' 
#' If there are errors it may be necessary to reprocess files. This function
#' will remove the fields "dbas_last_eval" and "dbas_spectral_library_sha256"
#' so that during the next processing, the files will be processed again.
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param queryBody The Query DSL code to use to select the docs in msrawfiles which need to be reset (as a `list`).
#' @param indexType Elasticsearch index, indicating the type of data stored in the index, can either be "dbas" or "dbas_is"
#' @param confirm logical, if True, will ask user to confirm the reset
#'
#' @export
#'
reset_eval <- function(escon, rfindex, queryBody, indexType = "dbas", confirm = TRUE) {
  
  switch(
    indexType, 
    dbas = {
      field <- "dbas_last_eval"
      field2 <- "dbas_spectral_library_sha256"
    },
    dbas_is = {
      field <- "dbas_is_last_eval"
      field2 <- "dbas_spectral_library_sha256"
    },
    nts = {
      field <- "nts_last_eval"
      field2 <- "nts_spectral_library_sha256"
    })
  
  if (confirm) {
    toChange <- elastic::Search(escon, rfindex, body = list(query = queryBody), 
                                size = 0)$hits$total$value
    message(toChange, " docs will be changed")
    okToGo <- readline("Proceed? (y/n)")
  } else {
    okToGo <- "y"
  }
  
  if (okToGo == "y") {
    elastic::docs_update_by_query(
      escon,
      rfindex,
      body = list(
        query = queryBody,
        script = sprintf("ctx._source.remove('%s'); ctx._source.remove('%s')", field, field2)
      )
    )
    
    # TODO this function should also delete all associated results in dbas indices (as an option)
    
  } else {
    message("Nothing was changed")
  }
  invisible(TRUE)
}




#' Get a field for a set of documents defined by id
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param index Name Elasticsearch index name 
#' @param esids Document IDs in the named index, must provide either esids or query
#' @param query Query to get docs in the form of a list, must provide either esids or query
#' @param fieldName field to extract
#' @param simplify Boolean, true will try to create a vector from the results
#' @param justone Boolean. If set to true, function will return only one result and will hang if cardinality > 1
#'
#' @return Vector or list of the field entries, returns NA is nothing found
#' @export
#'
get_field <- function(escon, indexName, esids = NULL, query = NULL, fieldName, simplify = T, justone = F) {
  if (!is.null(query)) {
    stopifnot(is.null(esids))
    idsL <- elastic::Search(escon, indexName, source = FALSE, body = query)$hits$hits
    esids <- vapply(idsL, "[[", i = "_id", character(1))
    if (length(esids) == 0)
      return(NA)
  }
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


#' Function factory just to avoid typing escon and index every time
#'
#' @param escon Connection object created with `elastic::connect` 
#' @param index Elasticsearch index name 
#'
#' @return get_field function with escon and index arguments set as default
get_field_builder <- function(escon, index) {
  function(esids, fieldName, simplify = T, justone = F) {
    get_field(escon = escon, indexName = index, esids = esids, 
              fieldName = fieldName, simplify = simplify, justone = justone)
  }
}











#' Removal of documents from dbas documents based on filename
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param index Elasticsearch index name 
#' @param filenames Vector of filenames to delete
#'
#' @return TRUE if successful
#' @export
#'
es_remove_by_filename <- function(escon, index, filenames) {
  # Must be a dbas index, not msrawfiles, otherwise this function would be bad!
  stopifnot(grepl("ntsp_index_dbas", index))
  stopifnot(is.character(filenames), length(filenames) > 0)
  qbod <- list(
    query = list(
      terms = list(
        filename = as.list(filenames)
      )
    )
  )
  suc <- TRUE
  if (elastic::Search(escon, index, body = qbod, size = 0)$hits$total$value == 0) {
    logger::log_info("No docs found in index {index}")
    return(invisible(FALSE))
  }
    
  
  tryCatch(
    res <- elastic::docs_delete_by_query(escon, index, body = qbod, refresh = "true"),
    error = function(cnd) {
      logger::log_error("Could not remove docs from {index}, \
                        {conditionMessage(cnd)}")
      suc <<- FALSE
    }
  )
  if (suc) {
    invisible(TRUE)  
  } else {
    invisible(FALSE)
  }
}


# Processing functions ####








# IS processing functions ####

#' Process all files in msrawfiles for IS
#' 
#' Will enter the results into the isindex. Will only process files which
#' are not included in isindex.
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param isindex 
#' @param ingestpth Path where the ingest.sh script is found
#' @param configfile Config file where the credentials for signing into elasticsearch are found
#' @param tmpPath Temporary save location
#' @param numCores Number of cores for parallel processing
#' @param rawfilesRootPath Path to where raw data is stored
#' @param numFilesToProcess If NULL (default) all remaining files are processed,
#' otherwise must be a positive integer.
#' @param idsToProcess If NULL (default) all remaining files are processed,
#' otherwise must be a character vector.
#' @param noIngest Logical, for testing purposes, no upload, just create json.
#'
#' @return Returns TRUE, invisbly, if successful
#' @export
#' 
process_is_all <- function(escon, rfindex, isindex, ingestpth, configfile, 
                           tmpPath = "~/tmp", numCores = 10,
                           rawfilesRootPath = "/scratch/nts/messdaten/",
                           numFilesToProcess = NULL,
                           idsToProcess = NULL, noIngest = FALSE) {
  startTime <- lubridate::now()
  
  # Run checks
  stopifnot(is.null(numFilesToProcess) || is.numeric(numFilesToProcess))
  stopifnot(is.null(idsToProcess) || is.character(idsToProcess))
  cr <- ntsportal::check_integrity_msrawfiles(
    escon = escon,
    rfindex = rfindex,
    locationRf = rawfilesRootPath
  )
  if (cr)
    log_info("{rfindex} integrity checks complete")
  
  # Get files to process
  if (is.null(idsToProcess))
    idsToProcess <- ntsportal::get_unevaluated(escon, rfindex, evalType = "dbas_is")
  
  if (!is.null(numFilesToProcess)) 
    idsToProcess <- idsToProcess[1:numFilesToProcess]
  
  log_info("Processing {length(idsToProcess)} files")
  if (length(idsToProcess) == 1) {
    featsBySample <- proc_is_one(escon = escon, rfindex = rfindex, esid = idsToProcess)
    if (is.null(featsBySample)) {
      return(FALSE)
    }
  } else {
    plan(multisession, workers = numCores)
    featsBySample <- furrr::future_map(idsToProcess, function(idx) {
      proc_is_one(escon = escon, rfindex = rfindex, esid = idx)
    })
    plan(sequential)
    
    featsAll <- do.call("c", featsBySample)
    if (is.null(featsAll)) {
      return(FALSE)
    }
    featsAll <- Filter(Negate(is.null), featsAll)
    stopifnot(length(featsAll) > 0)
  }
  
  jpth <- file.path(tmpPath, "is_dbas_temp.json")
  log_info("Writing json")
  jsonlite::write_json(featsAll, jpth, pretty = T, digits = NA, auto_unbox = T)
  if (!noIngest) {
    log_info("Ingesting")
    exitStatusIngest <- system(
      glue::glue("{ingestpth} {configfile} {isindex} {jpth}")
    )
    stopifnot(exitStatusIngest == 0)
    
    Sys.sleep(1)
    # Get filenames of processed files and check that data has been entered
    filenames <- vapply(featsAll, function(x) x$filename, character(1))
    docsFoundIndex <- elastic::Search(
      escon, isindex, 
      body = list(query = list(terms = list(filename = filenames))),
      size = 0
    )$hits$total$value
    stopifnot(docsFoundIndex > 0)
    
    # Add processing date to msrawfiles
    timeText <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "GMT")
    res <- es_add_value(
      escon, rfindex, list(ids = list(values = idsToProcess)),
      dbas_is_last_eval = timeText
    )
  }
  
  endTime <- lubridate::now()
  hrs <- round(as.numeric(endTime - startTime, units = "hours"))
  log_info("Processing IS complete in {hrs} h")
  invisible(TRUE)
}


#' Process IS in one file
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param esid ID of document in rfindex
#'
#' @return An ElasticSearch document (dbas-index-mapping) as a list
#' @export
proc_is_one <- function(escon, rfindex, esid) {
  
  get_field2 <- get_field_builder(escon = escon, index = rfindex)
  
  # Check that esid is present
  checkId <- elastic::Search(
    escon, rfindex, source = F, 
    body = list(query = list(ids = list(values = list(esid))))
  )$hits$hits
  if (length(checkId) != 1)
    return(NULL)
  
  isTab <- get_field2(esid, "dbas_is_table")
  stopifnot(length(isTab) == 1)
  ises <- read.csv(isTab)$name
  
  # Run dbas processing
  repo <- proc_esid(escon, rfindex, esid, compsProcess = ises)
  
  if (!exists("repo") || is.null(repo) || nrow(repo$ISresults) == 0) {
    pth <- get_field(escon, rfindex, esid[1], fieldName = "path")
    log_warn("No results found for {esid[1]}, {pth}")
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
    isr$date_measurement <- get_field2(esid, "date_measurement")
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


# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

