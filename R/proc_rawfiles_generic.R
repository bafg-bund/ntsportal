

#' Convert processing results to a list structure for ntsportal
#'
#' @description
#' Transforms a process output object (proco) which has a structure the workflow
#' used to a ntspl list object which has the structure for ntsportal
#' 
#' @param proco Processing output of class proco
#' @param coresBatch number of cores to use for parallelization (at the moment only 1)
#'
#' @details
#' This is a generic with methods for different workflow outputs. The output
#' for ntsworkflow has the class proco_nts (see make_ntspl.proco_nts).
#' 
#' @return a list object of class ntspl
#' @export
#'
make_ntspl <- function(proco, coresBatch = 1) {
  UseMethod("make_ntspl")
}

#' Save
#'
#' @param procList 
#' @param saveDir 
#' @param maxSizeGb maximum size of ntspl object before it is split into multiple
#' parts.
#'
#' @return will save json and return path
#' @export
#'
save_ntspl <- function(procList, saveDir, maxSizeGb) {
  UseMethod("save_ntspl")
}


#' Extract field from docs list
#' 
#' @description
#' a wrapper around vapply or lapply to get values from docs_source object
#' 
#' @param docsSrc docs_source object: list returned by elasticSearch API (only the _source part)
#' @param fieldName field to extract (length 1 character)
#' @param value FUN.VALUE argument for vapply or "unknown", in which case a list is returned
#' @param justone logical return only length one and warn if not all the same
#'
#' @return vector or list of the extracted value
#'
gf <- function(docsSrc, fieldName, value, justone = F) {
  # Check that field is present in all docs
  if (!all(vapply(docsSrc, function(x) fieldName %in% names(x), logical(1))))
    stop("Field ", fieldName, " not found in all docs")
  if (value == "unknown") {
    x <- lapply(docsSrc, "[[", i = fieldName)
  } else {
    x <- vapply(docsSrc, "[[", i = fieldName, FUN.VALUE = value)
  }
  
  if (justone && length(unique(x)) > 1) {
    warning("Extracting the field ", fieldName, " gave more than one unique value, choosing first")
  }
  if (justone) 
    x[1] else x
}



#' Ingest an ntsportal json file
#'
#' @param escon Connection object created with `elastic::connect`
#' @param ntsplJsonPath Path to json file, the filename must contain the alias into which
#'   the data must be ingested using the delimiters '-indn-' and '-bi-'
#' @param configPath Config file where the credentials for signing into 
#'   elasticSearch are found, see ntsportal-wiki
#' @param ingestScriptPath Path where the ingest.sh script is found
#' @param pauseTime number of seconds to wait before uploading (useful if running a loop)
#' @param verbose Print messages from ingest
#'
#' @return Will return true if all data has been ingested successfully, false otherwise
#' @export
#'
ingest_ntspl <- function(escon, ntsplJsonPath, configPath, ingestScriptPath, pauseTime = 2, verbose = F) {
  
  indexName <- stringr::str_match(ntsplJsonPath, "-indn-(.*)-bi-")[,2]
  stopifnot(is.character(indexName), nchar(indexName) > 8)
  stopifnot(elastic::index_exists(escon, indexName))
  # Build command
  command <- glue::glue("{ingestScriptPath} {configPath} {indexName} {ntsplJsonPath}")
  if (!verbose) 
    command <- paste0(command, " &> /dev/null")
  log_info("Ingest starting")
  system(command)
  log_info("Ingest complete, checking db")
  
  # Need to add a pause so that elastic returns ingested docs
  Sys.sleep(pauseTime)
  ntspList <- jsonlite::read_json(ntsplJsonPath)
  checkFiles <- unique(vapply(ntspList, "[[", character(1), i = "filename"))
  resp <- elastic::Search(
    escon, indexName, size = 0, 
    body = list(query = list(terms = list(filename = checkFiles)))
  )
  if (resp$hits$total$value == length(ntspList)) {
    log_info("All docs imported into ElasticSearch")
    return(TRUE)
  } else {
    log_warn("Ingested data not found in batch ingest of {ntsplJsonPath}")
    return(FALSE)
  }
}

#' Ingest a series of ntsportal json files
#'
#'
#' @description
#' given a folder of compressed json files, and a connection to elasticSearch,
#' this function will ingest all the json files sequentially
#' 
#'
#' @param escon Connection object created with `elastic::connect`
#' @param rfindex msrawfiles index name
#' @param resDir Where are the jsons found (must be compressed as json.gz)
#' @param configPath Credentials for ntsportal
#' @param ingestScriptPath Path where the ingest.sh script is found
#' @param type type of processing done, 'nts' or 'dbas'
#' @param pauseTime how much time to wait between uploading and checking, make 
#' sure you give it a lot of pause time for large uploads with many batches
#'
#' @return Successfully uploaded file paths
#' @export
#'
ingest_all_batches <- function(escon, rfindex, resDir, 
                               configPath = "~/config.yml", ingestScriptPath, 
                               type, pauseTime = 2) {
  
  fns <- list.files(resDir, pattern = ".json.gz$", full.names = T)
  stopifnot(length(fns) > 0)
  successful <- character()
  
  # The while loop will continue until all the batches have successfully been 
  # updated. At each failed update, remove files. 
  # loop While it is true that at least one fns is not in successful
  while (length(fns[!is.element(fns, successful)]) > 0) {
    # Take the first element of fns that is not in successful
    fn <- fns[!is.element(fns, successful)][1]
    tryCatch({
      system2("gunzip", fn)
      fn2 <- stringr::str_match(fn, "^(.*).gz$")[,2]
      # To record processing in msrawfiles, need to know msrawfiles index name and paths of files.
      docs <- jsonlite::read_json(fn2)
      paths <- unique(gf(docs, "path", character(1)))
      
      # To be able to delete these files in case of error need to have 
      # index name. This is unfortunately repeated in the ingest_ntspl function.
      indexName <- stringr::str_match(fn2, "--inda-(.*)-indn")[,2]
      stopifnot(is.character(indexName), nchar(indexName) > 8)
      stopifnot(elastic::index_exists(escon, indexName))
      
      done <- ingest_ntspl(escon, fn2, configPath, ingestScriptPath, pauseTime)
      
      record_proc(escon, rfindex, paths, type)
      
      successful <- append(fn, successful)
    },
    error = function(cnd) {
      log_warn("Error in uploading batch {fn2} with message: {conditionMessage(cnd)}")
      Sys.sleep(60)
      if (elastic::index_exists(escon, indexName)) {
        log_info("Removing any uploaded docs")
        res <- elastic::docs_delete_by_query(
          escon, indexName, body = list(query = list(terms = list(path = paths))))
        
      }
      message(res)
      Sys.sleep(120)
    },
    finally = {
      # recompress
      system2("gzip", fn2)
    })
    
  }
  successful
}


#' Record end of processing
#'
#' @param escon Connection object created with `elastic::connect`
#' @param rfindex msrawfiles index name
#' @param pathRawfiles The paths of the rawfiles (used as primary key to link msrawfiles with results)
#' @param type type of processing done, 'nts' or 'dbas'
#'
#' @return TRUE if successful
#'
record_proc <- function(escon, rfindex, pathRawfiles, type) {
  timeText <- format(Sys.time(), "%Y-%m-%d %H:%M:%S", tz = "GMT")
  
  # Get dbpath
  field <- switch(type, dbas = "dbas_spectral_library", nts = "nts_spectral_library")
  pathDb <- get_field(
    escon, rfindex, 
    query = list(query = list(terms = list(path = pathRawfiles))), 
    fieldName = field, 
    justone = T
  )
  
  cslHash <- system2("sha256sum", pathDb, stdout = TRUE)
  
  res <- switch(
    type,
    nts = es_add_value(
      escon, rfindex, 
      queryBody = list(terms = list(path = pathRawfiles)),
      nts_last_eval = timeText,
      nts_spectral_library_sha256 = cslHash
    ),
    dbas = es_add_value(
      escon, rfindex, 
      queryBody = list(terms = list(path = pathRawfiles)),
      dbas_last_eval = timeText,
      dbas_spectral_library_sha256 = cslHash
    )
  )
  
  if (exists("res") && length(res$failures) == 0) {
    return(invisible(TRUE))
  } else {
    return(invisible(FALSE))
  }
}


#' Get unprocessed files as batches
#'
#' @param allDocs entire msrawfiles index as docsList from elasticSearch
#' @param type type of processing, 'nts' or 'dbas'
#'
#' @return list of batches, each containing the docs for processing
#' @export
#'
get_unproc_batches <- function(allDocs, type) {
  docsSrc <- lapply(allDocs, "[[", i = "_source")
  field <- switch(type, nts = "nts_last_eval", dbas = "dbas_last_eval")
  notProc <- purrr::keep(docsSrc, function(doc) !is.element(field, names(doc)))
  # directories containing new files
  batches <- unique(dirname(gf(notProc, "path", character(1))))
  # If the doc is found in one of these dirs, then it is returned for processing
  docsOpen <- purrr::keep(allDocs, function(doc) dirname(doc[["_source"]]$path) %in% batches)
  dirs <- dirname(vapply(docsOpen, function(doc) doc[["_source"]][["path"]], character(1)))
  docsGrouped <- split(docsOpen, dirs)
  docsGrouped
}


#' Just get the whole msrawfiles index
#' 
#' @description
#' We just collect the whole index and do any further further manipulation in R
#' The sorting is done on the path field.
#'
#' @param escon Connection object created with `elastic::connect`
#' @param rfindex msrawfiles index name
#'
#' @return entire msrawfiles as an msrawfiles_docs_list
#' @export
#'
get_msrawfiles <- function(escon, rfindex) {
  # Collect entire msrawfiles index
  res <- es_search_paged(escon, rfindex, searchBody = list(
    query = list(match_all = stats::setNames(list(), character(0)))), 
    sort = "path")$hits$hits
  new_msrawfiles_docs_list(res)
}

#' @export
new_msrawfiles_docs_list <- function(x) {
  class(x) <- c("msrawfiles_docs_list", class(x))
  x
}

#' @export
`[.msrawfiles_docs_list` <- function(x, i) {
  new_msrawfiles_docs_list(NextMethod())
}

#' Create new index for DBAS results documents
#'
#' @param escon ElasticSearch connection object created by `elastic::connect`
#' @param rfindex Index name of msrawfiles index
#' @param type type of processing, 'nts' or 'dbas'
#' @param dateNum Provide a specific version code (YYMMDD), default is NULL (current date)
#' 
#' @return True (invisibly) when successful
#' @export
#'
create_index_all <- function(escon, rfindex, type, dateNum = NULL) {
  aliasType <- switch(type, nts = "nts_alias_name", dbas = "dbas_alias_name", stop("type must be one of nts or dbas"))
  # Get list of all aliases
  res <- elastic::Search(
    escon, rfindex, size = 0,
    body = list(
      aggs = list(
        aliases = list(
          terms = list(
            field = aliasType,
            size = 100
          )
        )
      )
    )
  )
  a <- vapply(res$aggregations$aliases$buckets, "[[", i = "key", character(1))
  for (i in a) {
    create_new_index(escon, rfindex, aliasName = i, dateNum = dateNum)
    Sys.sleep(1)
  }
  invisible(TRUE)
}

#' Get current index name based on the alias
#' 
#' The current date will be used in the index name
#' 
#' @param aliasName Name of alias used in elasticsearch
#' @param dateNum Provide a specific version code (YYMMDD), default is NULL (current date)
#'
#' @return Index name used in ElasticSearch
#'
index_from_alias <- function(aliasName, dateNum = NULL) {
  type <- stringr::str_extract(aliasName, "nts|dbas")
  if (is.null(dateNum)) {
    dateNum <- format(Sys.Date(), "%y%m%d")
  } else {
    stopifnot(grepl("\\d{6}", dateNum))
  }
  
  if (type == "dbas") {
    sub("^(ntsp_)dbas_", paste0("\\1index_dbas_v", dateNum, "_"), aliasName)
  } else if (type == "nts") {
    sub("^(ntsp_)nts_", paste0("\\1index_nts_v", dateNum, "_"), aliasName)
  } else {
    stop("must be nts or dbas")
  }
  
}

#' Create a new, empty index 
#' 
#' Will create a new index based on the alias with _v<date> appended after _dbas or _nts.
#' The new index name will be written to msrawfiles index.
#' 
#' @param escon elastic connection object created by elastic::connect
#' @param rfindex index name for rawfiles index
#' @param aliasName Name of alias used in elasticsearch
#' @param dateNum Provide a specific version code (YYMMDD), default is NULL (current date)
#'
#' @return Invisibly returns index name of the created index
#' @export
#'
create_new_index <- function(escon, rfindex, aliasName, dateNum = NULL) {
  
  type <- stringr::str_extract(aliasName, "nts|dbas")
  aliasNameType <- paste0(type, "_alias_name")
  indexNameType <- paste0(type, "_index_name")
  stopifnot(is.character(type), nchar(type) >= 3)
  # Name of index for this alias
  indNew <- index_from_alias(aliasName = aliasName, dateNum = dateNum)
  
  # Create new index
  resCrea <- switch(type, dbas = put_dbas_index(escon, indNew), nts = put_nts_index(escon, indNew))
  if (resCrea$acknowledged)
    logger::log_info("Acknowledged index creation {indNew}") else stop("Index creation {indNew} failed")
  
  # Add new index name to rfindex
  # use rlang list2 for programmatic list naming
  resNameChange <- es_add_value(
    escon, rfindex, 
    queryBody = list(term = list2(!!aliasNameType := aliasName)), 
    listToAdd = list2(!!indexNameType := indNew))
  
  if (resCrea$acknowledged && length(resNameChange$failures) == 0) {
    logger::log_info("Added index name to msrawfiles")
  } else {
    stop("Adding index name {indNew} to msrawfiles failed")
  }
  
  invisible(indNew)
}

