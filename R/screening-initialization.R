
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
  allRecords <- getAllMsrawfilesRecords(msrawfilesIndex)
  recordsToProcess <- getUnprocessedRecords(allRecords, screeningType)
  splitRecordsByDir(recordsToProcess)
}

getAllMsrawfilesRecords <- function(nameRawfilesIndex) {
  wholeIndexHits <- getWholeIndex(indexName = nameRawfilesIndex)
  getRecordsFromHits(hitsArray = wholeIndexHits, className = c("msrawfileRecord"))
}

getWholeIndex <- function(indexName) {
  matchAll <- list(
    query = list(match_all = stats::setNames(list(), character(0)))
  )
  es_search_paged(escon, indexName, searchBody = matchAll, sort = "path")$hits$hits
}

getRecordsFromHits <- function(hitsArray, className) {
  lapply(hitsArray, function(x) structure(x[["_source"]], class = className))
}

getUnprocessedRecords <- function(allRecords, screeningType) {
  indexToCheck <- switch(screeningType,
    nts = "ntsp_nts*",
    dbas = "ntsp_dbas*",
    dbasTest = "ntsp_dbas_unit_tests*",
    msrawfilesTest = "ntsp_index_msrawfiles_unit_tests*",
    stop("screeningType unknown")
  )
  allDirs <- extractDirs(allRecords)
  processedDirs <- getDirsInFeatureIndex(indexToCheck)
  dirsWithUnprocessed <- setdiff(allDirs, processedDirs)
  purrr::keep(allRecords, function(rec) dirname(rec$path) %in% dirsWithUnprocessed)
}

splitRecordsByDir <- function(docsToProcess) {
  dirs <- dirname(gf(docsToProcess, "path", character(1)))
  split(docsToProcess, dirs)
}

extractDirs <- function(allRecords) {
  unique(dirname(getField(allRecords, "path")))
}

getDirsInFeatureIndex <- function(indexName) {
  query <- list(size = 0, aggs = list(paths = list(terms = list(field = "path", size = 900000))))
  response <- elastic::Search(escon, indexName, body = query)$aggregations$paths$buckets
  unique(dirname(vapply(response, "[[", i = "key", character(1))))
}


