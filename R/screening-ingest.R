

#' Ingest of json.gz files into Elasticsearch
#' 
#' The processing output is a gzip-compressed json file for each batch (or more than one for large batches). These
#' are ingested into NTSPortal Elasticsearch.
#' 
#' @param path Path to single json.gz file or directory with json.gz files.
#'
#' @return list with one top-level entry per batch (json.gz file). The second level has one entry per
#' alias and this contains a character vector of the index name associated with that alias.
#' @export
#'
ingest <- function(path) {
  
  indexMappingPath <- fs::path_package("ntsportal", "extdata")
  dbComm <- getDbComm()
  indexTimeStamp <- format(lubridate::now(), "%y%m%d%H%M%S")
  
  jsonPaths <- getJsonFilePaths(path)
  aliasIndexPairs <- list()
  
  cli_progress_bar("Ingesting records", total = length(jsonPaths))
  for (pth in jsonPaths) {
    recs <- readJsonToRecords(pth)
    recs$addImportTime()
    
    aliasIndexPairs[[pth]] <- executePyIngestModule(recs, dbComm, indexTimeStamp, indexMappingPath)
    cli_progress_update()
  }
  walk(unlist(aliasIndexPairs), \(x) refreshTable(dbComm, x))
  return(aliasIndexPairs)
}

getJsonFilePaths <- function(path) {
  if (!(file.exists(path) || dir.exists(path))) 
    stop(paste("Path at", path, "does not exist."))
  
  if (dir.exists(path)) {
    filePaths <- list.files(path = path, recursive = TRUE, full.names = TRUE)
  } else if (file.exists(path)) {
    filePaths <- path
  }
  
  jsonFilesGz <- grep("json\\.gz$", filePaths, value = TRUE)
  if (length(jsonFilesGz) == 0) {
    stop(paste("No json.gz files found at", path, "."))
  }
  return(jsonFilesGz)
}

readJsonToRecords <- function(jsonPath, recordsConstructor = newNtspRecord) {
  tryCatch({
    stopifnot(length(jsonPath) == 1)
    newPath <- uncompressJson(jsonPath)
    records <- rjson::fromJSON(file = newPath)
    compressJson(newPath)
    Records$new(map(records, recordsConstructor))$addJsonPath(jsonPath)
  },
  error = function(cnd) {
    error = function(cnd) {
      log_error(
        "Error in readJsonToRecords during ingest of {jsonPath} error message: {conditionMessage(cnd)}"
      )
    }
  })
}

executePyIngestModule <- function(recs, dbComm, indexTimeStamp, indexMappingPath) {
  tryCatch(
    pyIngestModule$ingestRecords(recs$recs, dbComm@client, indexTimeStamp, indexMappingPath),
    error = function(cnd) {
      log_error(
        "Error in executePyIngestModule during ingest of {recs$jsonPath} error message: {conditionMessage(cnd)}"
      )
    }
  )
}

Records <- R6Class("Records", list(
  recs = list(),
  jsonPath = character(),
  initialize = function(ntspRecords) {
    stopifnot(
      inherits(ntspRecords, "list"),
      inherits(ntspRecords[[1]], "ntspRecord")
    )
    self$recs <- ntspRecords
    invisible(self)
  },
  print = function(...) {
    cat("Records (R6) of type ", class(self$recs[[1]]), " with ", length(self$recs), " ntspRecord(s).", sep = "")
  },
  addImportTime = function() {
    timeString <- format(lubridate::now("UTC"), "%Y-%m-%d %H:%M:%S")
    for (i in seq_along(self$recs))
      self$recs[[i]][["date_import"]] <- timeString
    invisible(self)
  },
  addJsonPath = function(path) {
    self$jsonPath <- path
    invisible(self)
  },
  changeDbasAliasName = function(newAlias) {
    for (i in seq_along(self$recs))
      self$recs[[i]][["dbas_alias_name"]] <- newAlias
    invisible(self)
  }
))

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
