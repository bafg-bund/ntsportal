




# Functions for the eval-rawfiles-dbas.R script ####


# Utility and local functions ####

# esids <- allFlsIds[[78]]
# cat(paste(shQuote(esids, type = "cmd"), collapse = ", "))
# rfindex <- RFINDEX
# fieldName <- "duration"
# source("~/connect-ntsp.R")

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



#' Get files which have not yet been processed
#' 
#' A processed file will have a time stamp. This function will return the ids
#' of all files without a time stamp for the given evaluation type.
#' 
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param evalType either dbas or dbas_is
#'
#' @return character vector of ids of documents
#' @export
#'
get_unevaluated <- function(escon, rfindex, evalType = "dbas") {
  stopifnot(evalType %in% c("dbas", "dbas_is"))
  fieldName <- switch(
    evalType,
    dbas = "dbas_last_eval",
    dbas_is = "dbas_is_last_eval"
  )
  
  resp <- ntsportal::es_search_paged(escon, rfindex, searchBody = sprintf('
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
      },
    "_source": ["path"]
    }
  ', fieldName), sort = "path:asc")
  
  vapply(resp$hits$hits, "[[", i = "_id", character(1))
}


#' Check that fields are the same for a set of documents
#'
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param esids ElasticSearch document IDs (character)
#' @param fieldName 
#' @param onlyNonBlanks 
#'
#' @return TRUE if the field is uniform accross the docs, false otherwise
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
    logger::log_error("Non-uniformity in batch starting with id {esids[1]} and \
                     field {fieldName}")
    return(FALSE)
  } 
}

res_field <- function(res, field, value = character(1)) {
  vapply(
    res$hits$hits, 
    function(x) x[["_source"]][[field]], value)
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



#' Normalize ms2 spectrum to the maximum intensity
#'
#' @param x Spectrum as a data.frame with columns mz and int 
#' @param precursormz Molecular ion precursor m/z
#' @param mztol Tolerance of m/z 
#' @param noiselevel Intensity level to set as noise 
#'
#' @return Normalized spectrum as a data.frame
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
#' @param x Spectrum as a data.frame with columns mz and int 
#' @param precursormz Molecular ion precursor m/z
#' @param precursorInt Molecular ion precursor intensity 
#' @param noiselevel Intensity level to set as noise 
#'
#' @return Normalized spectrum as a data.frame
norm_ms1 <- function(x, precursormz, precursorInt, noiselevel = 0.1) {
  # remove noise
  x <- x[x$int >= noiselevel, ]
  if (length(x) == 0 || nrow(x) == 0)
    return(NULL)
  data.frame(mz = x$mz, int = x$int / precursorInt)
}



#' Remove fields in a doc that are or contain NAs
#'
#' @param doc A single document (of a NT feature) in the form of a list
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
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
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




fileScanDbas(msrawfileRecord, compsToProcess) {
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
  # TODO Change CSL to give bfg retention times the new name "bfg_nts_rp1"
  if (msrawfileRecord$chrom_method == "bfg_nts_rp1") {  # new name
    scannerObj$changeSettings("chromatography", "dx.doi.org/10.1016/j.chroma.2015.11.014")  # old name
  } else {
    scannerObj$changeSettings("chromatography", msrawfileRecord$chrom_method)
  }
  scannerObj
}

runScanningDbas <- function(scannerObj, compsToProcess) {
  tryCatch({
    suppressMessages(scannerObj$process_all(comp_names = compsProcess))
    scannerObj$clearData()
  }, 
  error = function(cnd) {
    log_error("Processing error in file: {scannerObj$rawfiles},
                  error message: {conditionMessage(cnd)}")
  })
}

#' Process an MS measurement file
#' 
#' @description
#' Takes an esid from the rawfiles index and carries out dbas processing (see details) for 
#' that file, returns ntsworkflow::Report class object with results.
#' 
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param esid ElasticSearch document IDs in the rawfiles index (character)
#' 
#' @details
#' # dbas processing
#' dbas processing means to take mzXML measurement file and analyse for compounds found
#' in the spectral library, peaks are integrated and spectra are extracted and saved
#' in the Report object.
#' 
#' 
#' @return an object of class ntsworkflow::Report
#' @export
proc_esid <- function(escon, rfindex, esid, compsProcess = NULL) { 

  # Connect to ES to retrieve doc source, extract source from API response
  stopifnot(length(esid) == 1)
  dc <- elastic::docs_get(escon, rfindex, esid, verbose = F)
  dc <- dc$`_source`
  
  # Start ntsworkflow::Report object to do the processing of this file
  # Transfer settings from doc source to Report object
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
  
  # Inconsistency in naming of chromatographic method between NTSPortal and CSL
  # Corrects the naming so that it works for CSL
  # TODO Change CSL to give bfg retention times the new name "bfg_nts_rp1"
  if (dc$chrom_method == "bfg_nts_rp1") {  # new name
    dbas$changeSettings("chromatography", "dx.doi.org/10.1016/j.chroma.2015.11.014")  # old name
  } else {
    dbas$changeSettings("chromatography", dc$chrom_method)
  }
  
  # Begin the actual processing
  dbas$changeSettings("numcores", 1)
  crash <- FALSE
  tryCatch(
    suppressMessages(dbas$process_all(comp_names = compsProcess)), 
    error = function(cnd) {
      log_error("Processing error in file with id {esid}. File path: {dc$path},
                error message: {conditionMessage(cnd)}")
      crash <<- TRUE
    }
  )
  if (crash)
    return(NULL)
  
  # Clear data will remove the meas. file from memory
  dbas$clearData()
  
  # Return Report object
  dbas
}


#' Process a batch of MS measurement files stored in the msrawfiles index
#' 
#' Takes a vector of esids and performs dbas on these as a batch. Files are
#' evaluated using the settings in the msrawfiles index. Results are ingested to
#' the indicated dbas results index
#' 
#' @param escon Elasticsearch connection object created by `elastic::connect`
#' @param rfindex Name of rawfiles index
#' @param esids ElasticSearch document IDs (character)
#' @param tempsavedir Temporary save location
#' @param ingestpth Path where the ingest.sh script is found
#' @param configfile Config file where the credentials for signing into elasticsearch are found
#' @param coresBatch Number of cores to use in a signal batch
#' @param noIngest Logical, for testing purposes, no upload, just create json. Will also return JSON (invisibly)
#'
#' @return Function is run for side-effects but returns number of documents uploaded
#' @export
proc_batch <- function(escon, rfindex, esids, tempsavedir, ingestpth, configfile, 
                       coresBatch, noIngest = FALSE) {
  
  # Define functions
  # Create a get_field function with some default parameters
  get_field2 <- get_field_builder(escon = escon, index = rfindex)
  
  
  # Define Variables
  # These pollute the comp_group field and need to be removed
  SPEC_SOURCES <- c("BfG", "LfU", "UBA")
  batchStartTime <- lubridate::now()
  
  # Create path for saving report files
  savename <- gsub("[/\\.]", "_", dirname(get_field2(esids, "path"))[1])
  savename <- paste0("dbas-batch--", savename, "--", format(Sys.time(), "%y%m%d"), ".report")
  savename <- file.path(tempsavedir, savename)
  
  # Get path to collective spectral library (CSL)
  dbPath <- get_field2(esids, "dbas_spectral_library", justone = T)

  # Get path to batch (measurement files)
  dr <- paste(unique(dirname(get_field2(esids, "path", justone = F))), collapse= ", ")
  
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
  
  
  log_info("Starting batch {dr}")

  tryCatch({
    # Step 1 - Screening ####
    tryCatch({
        if (rstudioapi::isAvailable()) {
          plan(multisession, workers = coresBatch)  
        } else {
          plan(multicore, workers = coresBatch)
        }
        repLt <- furrr::future_map(
          esids, 
          proc_esid, 
          escon = escon, 
          rfindex = rfindex
        )
        plan(sequential)
      },
      error = function(cnd) {
        log_warn("Error in screening for batch with id {esids[1]}")
        message(cnd)
      }
    )
    
    if (any(vapply(repLt, is.null, logical(1))) || 
        any(vapply(repLt, inherits, logical(1), what = "try-error"))) {
      errorEsids <- c(
        esids[vapply(repLt, is.null, logical(1))],
        esids[vapply(repLt, inherits, logical(1), what = "try-error")]
      )
      errorFiles <- get_field2(errorEsids, "path")
      errorFilesClps <- paste(errorFiles, collapse = "\n")
      log_warn("Error in files {errorFilesClps}")
    }
    # remove errors
    repLt <- Filter(Negate(is.null), repLt)
    repLt <- Filter(function(x) Negate(inherits)(x, what = "try-error"), repLt)
    
    if (length(repLt) == 0) {
      log_warn("No results for batch starting with file {get_field2(esids, 'path')[1]}")
      record_end_processing(escon, rfindex, esids, dbPath, batchStartTime)
      return(0)
    }
    
    # Remove reports with no peaks
    nothingFound <- vapply(repLt, function(x) nrow(x$peakList) == 0, logical(1))
    
    if (any(nothingFound)) {
      errorEsids <- esids[nothingFound]
      nothingFiles <- get_field2(errorEsids, "path")
      message(sprintf("\nFiles\n%s\nhave no hits.", 
                      paste(nothingFiles, collapse = "\n")))
    }
    
    repLt <- Filter(function(x) nrow(x$peakList) != 0, repLt)  
    
    if (length(repLt) == 0) {
      log_warn("No results for batch starting with file {get_field2(esids, 'path')[1]}")
      record_end_processing(escon, rfindex, esids, dbPath, batchStartTime)
      return(0)
    }
    
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
    resM$clearAndSave(F, savename, clearData = F)
    numPeaks <- nrow(resM$peakList)
    rm(repLt)
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
    
    # Rename Report
    dbas <- resM
    dbas$changeSettings("numcores", 1)
    
    log_info("Blank correction")
    
    if (!is.null(bregex) && any(grepl(bregex, dbas$rawFiles))) {
      dbas$deleteBackground(grep(bregex, dbas$rawFiles, invert = T), 
                            grep(bregex, dbas$rawFiles))
      dbas$remRawFiles(grep(bregex, dbas$rawFiles))
      esids_blank <- esids[get_field2(esids, "blank", simplify = T)]
      esids <- esids[!get_field2(esids, "blank", simplify = T)]
    } else {
      esids_blank <- NULL
      log_info("No blanks found in {dr}")
    }
    
    log_info("Remove false positives")
    
    # Get list of false positives (will just add all of the fps)
    bfps <- get_field2(esids, "dbas_fp", simplify = F)
    # TODO in the future, delete fps on a by-sample basis
    # for now, just add them all together into a big list
    bfps <- unique(unlist(bfps))
    
    bmindet <- unique(get_field2(esids, "dbas_minimum_detections"))
    stopifnot(length(bmindet) == 1, is.integer(bmindet))
    
    # Get rare compounds from a Report
    # Get the compounds which are found fewer than min_freq
    # repo ntsworkflow::Report class object
    # min_freq Minimum number of detections
    # return Names of compounds with FEWER than the min_freq of detections
    get_rare <- function(repo, min_freq) {
      pl <- repo$peakList
      finds <- by(pl, pl$comp_name, nrow)
      rare <- which(finds < min_freq)
      names(rare)
    }
    
    if (bmindet > 1)
      bfps <- append(bfps, get_rare(dbas, bmindet))
    
    # Append compounds which are not found in at least x out of y replicates
    # where x is dbas_minimum_detections
    # dbas_minimum_detections therefore refers to the whole batch as well as
    # one set of replicates (see previous filter).
    # The total number of replicates for each sample is irrelevant
    brepr <- unlist(unique(get_field2(esids, "dbas_replicate_regex")))
    
    if (!is.null(brepr) && !is.na(brepr)) {
      pl <- dbas$peakList
      pl$orig <- stringr::str_replace(pl$samp, brepr, "\\1")
      
      # Get the total number of replicates for each sample
      # repTotal <- data.frame(
      #   orig = unique(pl$orig),
      #   number = tapply(pl$samp, pl$orig, function(p) length(unique(p)))
      # )
      
      repCount <- by(pl, list(pl$comp_name, pl$orig), nrow)
      repCount <- array2DF(repCount)
      repCount <- repCount[!is.na(repCount$Value), ]
      
      # We take the average number of times a compound is found in y replicates,
      # that way, if in one set of reps it doesn't get the required number of 
      # detections, it still doesn't count as an FP
      compCount <- data.frame(
        comp_name = unique(repCount$Var1),
        count = round(tapply(repCount$Value, repCount$Var1, mean))
      )
      repFps <- compCount[compCount$count < bmindet, "comp_name"]
      bfps <- append(bfps, repFps)
    }
    
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
    
    dbas <- ntsworkflow::loadReport(F, newsavename)  
    #dbas <- ntsworkflow::loadReport(F, "tests/dbas-eval/temp-files/dbas-batch--_srv_cifs-mounts_g2_G_G2_HRMS_Messdaten_koblenz_wasser_2019_201904_pos--240429_i.report")  
    
    
    # Step 3 - Conversion ####
    log_info("Collecting data for json export")
    
    compData <- dbas$integRes[, c("samp", "comp_name", "adduct", "isotopologue",
                                  "int_a", "real_mz", "real_rt_min")]
    compData$samp <- basename(compData$samp)
    
    bist <- unique(get_field2(esids, "dbas_is_table"))
    stopifnot(length(bist) == 1)
    
    bisn <- unique(get_field2(esids, "dbas_is_name"))
    stopifnot(length(bisn) == 1)
    
    isData <- dbas$ISresults[dbas$ISresults$IS == bisn, c("samp", "int_a")]

    # Normalize intensities
    dat <- merge(compData, isData, by = "samp", suffix = c("", "_IS"))
    # verify columns are numeric
    dat[, c("int_a", "int_a_IS")] <- lapply(dat[, c("int_a", "int_a_IS")], as.numeric)
    dat$norm_a <- round(dat$int_a / dat$int_a_IS, 4)
    dat$area_normalized <- dat$norm_a  # norm_a and area_normalized are exactly the same within this script. keep both?
    
    if (length(dat$int_a_IS) == 0)
      stop("IS not found in docs ", paste(esids, collapse = ", "))
    
    if (!all(is.na(dat$int_a_IS))) {
      rstdev <- round((sd(dat$int_a_IS)/mean(dat$int_a_IS)), 2)
      log_info("Die relative SD des internen Standards beträgt ", 100*rstdev, "%")
      # TODO The script must automatically decide what to do.
      if  (100*rstdev >= 10) 
        log_warn("Die Standardabweichung des internen Standards ist über Grenzwert von 10%")
    }
    
    # Build average area from replicates
    # brepr is the batch replicate regex which was defined for the replicate filter
    if (!is.null(brepr) && !is.na(brepr)) {
      log_info("Building replicate averages for testing")
      # using the regex, give all replicate samples the same name 
      # (best when replicates are indicated by _1 at the end)
      dat$reps <- stringr::str_replace(dat$samp, brepr, "\\1")
      averages <- by(dat, list(dat$reps, dat$comp_name), function(part) {
        comp1 <- part$comp_name[1]
        ad1 <- part$adduct[1]
        isot1 <- part$isotopologue[1]
        samp1 <- part$samp[1]
        average_int_a <- mean(part$int_a, na.rm = T)
        average_norm_a <- mean(part$norm_a, na.rm = T)
        standard_deviation_norm_a <- sd(part$norm_a, na.rm = T)
        rsd_norm_a <- standard_deviation_norm_a/average_norm_a
        average_int_a_IS <- mean(part$int_a_IS, na.rm = T)
        data.frame(
          samp = samp1, comp_name = comp1, adduct = ad1, isotopologue = isot1, 
          int_a = average_int_a, int_a_IS = average_int_a_IS, norm_a = average_norm_a, 
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
      
      # Warning if sd for any comp is high
      if (any((100*datTemp$rsd_norm_a >= 30), na.rm = T))   {
        log_info("The SD of a detection in the replicates may be high")
        probleme <- subset(datTemp, 100*rsd_norm_a >= 30, comp_name, drop = TRUE)
        problemeProben <- subset(datTemp, 100*rsd_norm_a >= 30, samp, drop = TRUE)
        log_info("Problematic detections:")
        for (i in seq_along(probleme)) 
          message(probleme[i], " in sample ", problemeProben[i])
      }
      
      dat$reps <- NULL
    }
    
    # Round all int columns to 3 sig figs
    dat$int_a <- signif(dat$int_a, 3)
    dat$norm_a <- signif(dat$norm_a, 3)
    dat$area_normalized <- signif(dat$area_normalized, 3)
    dat$int_a_IS <- signif(dat$int_a_IS, 3)
    
    # Round mz
    dat$real_mz <- round(dat$real_mz, 4)
    
    # Get start time for sample
    idSamp <- data.frame(
      id = esids,
      samp = basename(get_field2(esids, "path")),
      start = get_field2(esids, "start"),
      date_measurement = get_field2(esids, "date_measurement")
    )
    
    dat <- merge(dat, idSamp, by = "samp", all.x = T)
    stopifnot(all(!is.na(dat$start)))
    
    # Get other data
    # CAS-RN
    cas <- dbas$peakList[, c("comp_CAS", "comp_name")]
    cas <- cas[!duplicated(cas),]
    dat <- merge(dat, cas, by = "comp_name", all.x = T)
    
    dat$date_import <- round(as.numeric(Sys.time()))  # epoch_seconds
    dat$duration <- get_field2(esids, "duration", justone = T)
    
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
    colnames(dat) <- gsub("^real_mz$", "mz", colnames(dat))
    colnames(dat) <- gsub("^real_rt_min$", "rt", colnames(dat))
    
    # Make dat into list to allow for nested data structure
    rownames(dat) <- NULL
    datl <- split(dat, seq_len(nrow(dat))) 
    datl <- lapply(datl, as.list)
    
    # Add compound information 
    sdb <- con_sqlite(dbPath)
    comptab <- tbl(sdb, "compound") %>% collect()
    grouptab <- tbl(sdb, "compound") %>% 
      select(name, compound_id) %>% 
      left_join(tbl(sdb, "compGroupComp"), by = "compound_id") %>% 
      left_join(tbl(sdb, "compoundGroup"), by = "compoundGroup_id") %>% 
      select(name.x, name.y) %>% 
      rename(compname = name.x, groupname = name.y) %>% 
      collect()
    
    # Using '$' will fail without an error because it implements greedy matching 
    # (inchi and inchikey)
    datl <- lapply(datl, function(doc) {
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
    DBI::dbDisconnect(sdb)
    
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
    
    datl <- lapply(datl, function(doc) { # doc <- datl[[1]]
      #browser(expr = doc$name == "10-Hydroxycarbamazepine")
      idtemp <- subset(
        dbas$peakList, 
        comp_name == doc$name & 
          samp == doc$filename &
          adduct == doc$adduct &
          isotopologue == doc$isotopologue, peakID, drop = T)
      # If the peakID was not found, then this means there is no entry for this
      # peak in the peaklist. The rest of the entries (eic, ms1, ms2) are skipped.
      if ( length(idtemp) == 0 || !is.numeric(idtemp))
        return(doc) 
  
      # if more than one peak matches (isomers), which should be marked by peak A, B etc, 
      # choose the peak with the highest intensity
      if (length(idtemp) > 1) { 
        best <- which.max(subset(dbas$peakList, peakID %in% idtemp, int_a, drop = T))
        idtemp <- subset(dbas$peakList, peakID %in% idtemp, peakID, drop = T)[best]
        doc$comment <- paste(doc$comment, "isomers found")
      } else {
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
            ms1temp <- norm_ms1(ms1temp, doc$mz, inttemp)
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
            ms2temp <- norm_ms2(ms2temp, doc$mz)
            if (!is.null(ms2temp)) {
              ms2temp$mz <- round(ms2temp$mz, 4)
              ms2temp$int <- round(ms2temp$int, 4)
              rownames(ms2temp) <- NULL
              doc$ms2 <- ms2temp
            }
          }
        }
      }
      # TODO even for peaks that are not in the peaklist (no ms2) still have an
      # eic and an ms1. This information could still be extracted. 
      doc
    })  
    
    # Add tags if available
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
    
    # Write JSON ####
    jsonPath <- sub("\\.report$", ".json", newsavename)  # jsonPath <- "tests/bimmen.json"
    
    jsonlite::write_json(datl, jsonPath, pretty = T, digits = NA, auto_unbox = T)
    
    # Step 4 - Ingest ####
    if (!noIngest) {
      bindex <- get_field2(esids, "dbas_index_name", justone = T)
      log_info("Ingest starting")
      if (rstudioapi::isAvailable()) {
        system(glue::glue("{ingestpth} {configfile} {bindex} {jsonPath}"))
      } else {
        system(
          glue::glue("{ingestpth} {configfile} {bindex} {jsonPath} &> /dev/null")
        )
      }
      log_info("Ingest complete, checking database for results")
      
      # Need to add a pause so that elastic returns ingested docs
      Sys.sleep(1)
      # Check that everything is in the database
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
        esidsAll <- c(esids, esids_blank)
        record_end_processing(escon, rfindex, esidsAll, dbPath, batchStartTime)
        log_info("Completed batch starting with id {esidsAll[1]}, file {checkFiles[1]}")
      } else {
        log_error("Ingested data not found in batch starting with id {esids[1]}")
      }
    } else {
      return(invisible(datl))
    }
  },
  error = function(cnd) {
    log_error("Fail proc_batch in batch starting with id {esids[1]}: {conditionMessage(cnd)}")
  }
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
                           tmpPath = "/scratch/nts/tmp", numCores = 10,
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

