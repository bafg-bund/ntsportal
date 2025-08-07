

#' Ingest of RDS files into Elasticsearch
#' 
#' The processing output is an RDS file for each batch (or more than one for large batches). These
#' are ingested into NTSPortal Elasticsearch.
#' 
#' @param path Path to single RDS file or directory with RDS files.
#' @param ingestPipeline ID of ingest pipeline to use (default is none used)
#' 
#' @details
#' \strong{Ingest pipelines}
#' Feature documents are enriched with sample and analytical method information using an ingest pipeline.
#' Use `ingestPipeline` to specify which ingest pipeline to use (must have been previously submitted to Elasticsearch)
#' 
#' @return list with one top-level entry per batch (RDS file). The second level has one entry per
#' alias and this contains a character vector of the index name associated with that alias.
#' @export
#'
ingest <- function(path, ingestPipeline = NULL) {
  if (!testForPipeline(ingestPipeline))
    stop("Ingest pipeline ", ingestPipeline, " does not exist")
  indexMappingPath <- fs::path_package("ntsportal", "mappings")
  dbComm <- getDbComm()
  indexTimeStamp <- format(lubridate::now(), "%y%m%d%H%M%S")
  
  rdsPaths <- getRdsFilePaths(path)
  aliasIndexPairs <- list()
  
  cli_progress_bar("Ingesting records", total = length(rdsPaths))
  for (pth in rdsPaths) {
    recs <- readRdsToRecords(pth)
    recs$addImportTime()
    
    aliasIndexPairs[[pth]] <- executePyIngestModule(recs, dbComm, indexTimeStamp, indexMappingPath, ingestPipeline)
    cli_progress_update()
  }
  walk(unlist(aliasIndexPairs), \(x) refreshTable(dbComm, x))
  taskId <- executeEnrichPolicy("date-import-policy")
  message("Refreshed date-import-policy with task id: ", taskId)
  return(aliasIndexPairs)
}

getRdsFilePaths <- function(path) {
  if (!(file.exists(path) || dir.exists(path))) 
    stop(paste("Path at", path, "does not exist."))
  
  if (dir.exists(path)) {
    filePaths <- list.files(path = path, recursive = TRUE, full.names = TRUE)
  } else if (file.exists(path)) {
    filePaths <- path
  }
  
  rdsFiles <- grep(".*ntsportal-featureRecord.*\\.RDS$", filePaths, value = TRUE)
  if (length(rdsFiles) == 0) {
    stop(paste("No RDS files found at", path, "."))
  }
  rdsFiles
}

readRdsToRecords <- function(rdsPath, recordsConstructor = newNtspRecord) {
  tryCatch({
    stopifnot(length(rdsPath) == 1)
    records <- readRDS(rdsPath)
    Records$new(map(records, recordsConstructor))$addRdsPath(rdsPath)
  },
  error = function(cnd) {
    error = function(cnd) {
      log_error(
        "Error in readRdsToRecords during ingest of {rdsPath} error message: {conditionMessage(cnd)}"
      )
    }
  })
}

executePyIngestModule <- function(recs, dbComm, indexTimeStamp, indexMappingPath, ingestPipeline) {
  tryCatch({
    pyIngestModule$ingestRecords(recs$recs, dbComm@client, indexTimeStamp, indexMappingPath, pipeline = ingestPipeline)},
    error = function(cnd) {
      log_error(
        "Error in executePyIngestModule during ingest of {recs$rdsPath} error message: {conditionMessage(cnd)}"
      )
    }
  )
}

Records <- R6Class("Records", list(
  recs = list(),
  rdsPath = character(),
  initialize = function(ntspRecords) {
    stopifnot(
      inherits(ntspRecords, "list"),
      inherits(ntspRecords[[1]], "ntspRecord")
    )
    self$recs <- lapply(ntspRecords, unclass) # enable easy conversion to Python dict
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
  addRdsPath = function(path) {
    self$rdsPath <- path
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
