


getUnprocessedMsrawfilesRecords <- function(nameMsrawfilesIndex, screeningType) {
  allRecords <- getAllMsrawfilesRecords(nameMsrawfilesIndex)

  recordsToProcess <- getRecordsToProcess(allRecords, screeningType)
  recordsInBatches <- splitRecordsByDir(recordsToProcess)
  recordsInBatches
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

getRecordsToProcess <- function(allRecords, screeningType) {
  fieldToCheck <- switch(screeningType,
    nts = "nts_last_eval",
    dbas = "dbas_last_eval",
    stop("screeningType unknown")
  )
  unprocessedRecords <- purrr::keep(allRecords, function(rec) !is.element(fieldToCheck, names(rec)))
  dirsWithUnprocessed <- unique(dirname(gf(unprocessedRecords, "path", character(1))))
  purrr::keep(allRecords, function(rec) dirname(rec$path) %in% dirsWithUnprocessed)
}

splitRecordsByDir <- function(docsToProcess) {
  dirs <- dirname(gf(docsToProcess, "path", character(1)))
  split(docsToProcess, dirs)
}
