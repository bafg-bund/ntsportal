

getSelectedMsrawfileBatches <- function(msrawfilesIndex, batchDirs) {
  allRecords <- getAllMsrawfilesRecords(msrawfilesIndex)
  recordsToProcess <- getSelectedRecords(allRecords, batchDirs)
  splitRecordsByDir(recordsToProcess)
}

getSelectedRecords <- function(allRecords, dirsToKeep) {
  dirsToKeep <- normalizePath(list.dirs(dirsToKeep)) 
  purrr::keep(allRecords, function(record) dirname(record$path) %in% dirsToKeep)
}

getUnprocessedMsrawfileBatches <- function(msrawfilesIndex, screeningType) {
  ntspVersion <- stringr::str_match(msrawfilesIndex, "^ntsp(\\d\\d\\.\\d)")[,2]
  stopifnot(grepl("^\\d\\d\\.\\d$", ntspVersion))
  allRecords <- getAllMsrawfilesRecords(msrawfilesIndex)
  recordsToProcess <- getUnprocessedRecords(allRecords, screeningType, ntspVersion)
  splitRecordsByDir(recordsToProcess)
}

getAllMsrawfilesRecords <- function(msrawfilesIndex) {
  dbComm <- getDbComm()
  getTableAsRecords(dbComm, msrawfilesIndex, recordConstructor = newMsrawfilesRecord)
}

getRecordsFromHits <- function(hitsArray, className) {
  lapply(hitsArray, function(x) structure(x[["_source"]], class = className))
}

getUnprocessedRecords <- function(allRecords, screeningType, ntspVersion) {
  indexToCheck <- switch(screeningType,
    nts = glue("ntsp{ntspVersion}_nts*"),
    dbas = glue("ntsp{ntspVersion}_dbas*"),
    dbasTest = glue("ntsp{ntspVersion}_dbas_unit_tests*"),
    stop("screeningType unknown")
  )
  allDirs <- extractDirs(allRecords)
  processedDirs <- getDirsInFeatureIndex(indexToCheck)
  dirsWithUnprocessed <- setdiff(allDirs, processedDirs)
  purrr::keep(allRecords, function(rec) dirname(rec$path) %in% dirsWithUnprocessed)
}

splitRecordsByDir <- function(recsToProcess) {
  paths <- getField(recsToProcess, "path")
  if (length(paths) > 0) {
    dirs <- dirname(getField(recsToProcess, "path"))
  } else {
    dirs <- character(0)
  }
  split(recsToProcess, dirs)
}

extractDirs <- function(allRecords) {
  unique(dirname(getField(allRecords, "path")))
}

getDirsInFeatureIndex <- function(indexName) {
  dbComm <- getOption("ntsportal.dbComm")()
  allPaths <- getUniqueValues(dbComm, indexName, "path", maxLength = 9e5)
  unique(dirname(allPaths))
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
