

#' Ingest of json.gz files into Elasticsearch
#' 
#' @param path Path to single json file or directory with json files.
#'
#' @return Returns pairs for all unique aliases and the respective indices
#' @export
#'
ingestJson <- function(path) {
  
  indexMappingPath <- fs::path_package("ntsportal", "extdata")
  dbComm <- getDbComm()
  indexTimeStamp <- format(lubridate::now(), "%y%m%d%H%M%S")
  
  jsonPaths <- getJsonFilePaths(path)
  aliasIndexPairs <- list()
  
  cli_progress_bar("Ingesting records", total = length(jsonPaths))
  for (pth in jsonPaths) {
    recs <- readJsonToRecords(pth)
    recs$addImportTime()
    aliasIndexPairs[[pth]] <- pyIngestModule$ingestRecords(recs$recs, dbComm@client, indexTimeStamp, indexMappingPath)
    cli_progress_update()
  }
  
  return(aliasIndexPairs)
}

readJsonToRecords <- function(jsonPath, recordsConstructor = newNtspRecord) {
  stopifnot(length(jsonPath) == 1)
  newPath <- uncompressJson(jsonPath)
  records <- rjson::fromJSON(file = newPath)
  compressJson(newPath)
  Records$new(map(records, recordsConstructor))
}

Records <- R6Class("Records", list(
  recs = list(),
  initialize = function(ntspRecords) {
    stopifnot(
      inherits(ntspRecords, "list"),
      inherits(ntspRecords[[1]], "ntspRecord")
    )
    self$recs <- ntspRecords 
  },
  print = function(...) {
    cat("Records (R6) of type ", class(self$recs[[1]]), " with ", length(self$recs), " ntspRecord(s).", sep = "")
  },
  addImportTime = function() {
    timeString <- format(lubridate::now("UTC"), "%Y-%m-%d %H:%M:%S")
    for (i in seq_along(self$recs))
      self$recs[[i]][["date_import"]] <- timeString
  }
))



getJsonFilePaths <- function(path) {
  if (!(file.exists(path) || dir.exists(path))) 
    stop(paste("Path at", path, "does not exist."))
  
  if (dir.exists(path)) {
    filePaths <- list.files(path = path, recursive = TRUE, full.names = TRUE)
  } else if (file.exists(path)) {
    filePaths <- path
  }
  
  jsonFilesGz <- grep("\\.gz$", filePaths, value = TRUE)
  if (length(jsonFilesGz) == 0) {
    stop(paste("No .gz files found at", path, "."))
  }
  return(jsonFilesGz)
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
