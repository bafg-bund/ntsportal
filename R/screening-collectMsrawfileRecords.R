

getSelectedMsrawfileBatches <- function(msrawfilesIndex, rootDirs) {
  batchNames <- getAllBatchesInDir(msrawfilesIndex, rootDirs)
  if (length(batchNames) == 0)
    stop("No batches found in dir ", paste(rootDirs, collapse = ", "))
  map(batchNames, \(batchName) getSelectedMsrawfileBatch(msrawfilesIndex, batchName))
}

getAllBatchesInDir <- function(msrawfilesIndex, batchDirs) {
  dirsToKeep <- list_c(map(batchDirs, \(x) normalizePath(list.dirs(x))))
  keep(dirsToKeep, \(x) isBatchDir(msrawfilesIndex, x))
}

isBatchDir <- function(msrawfilesIndex, batchDir) {
  getNrow(getDbComm(), msrawfilesIndex, searchBlock = list(query = list(term = list(batchname = batchDir)))) > 0
}

getSelectedMsrawfileBatch <- function(msrawfilesIndex, batchName) {
  queryDsl <- list(query = list(term = list(batchname = batchName)))
  getTableAsRecords(getDbComm(), msrawfilesIndex, searchBlock =  queryDsl, sortField = "start", 
                    recordConstructor = newMsrawfilesRecord)
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

getUnprocessedRecords <- function(allRecords, screeningType, ntspVersion) {
  indexToCheck <- switch(screeningType,
    feature = glue("ntsp{ntspVersion}_feature*"),
    feature_test = glue("ntsp{ntspVersion}_feature_unit_tests*"),
    stop("screeningType unknown")
  )
  allDirs <- extractDirs(allRecords)
  processedDirs <- getDirsInFeatureIndex(indexToCheck)
  dirsWithUnprocessed <- setdiff(allDirs, processedDirs)
  purrr::keep(allRecords, function(rec) dirname(rec$path) %in% dirsWithUnprocessed)
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
