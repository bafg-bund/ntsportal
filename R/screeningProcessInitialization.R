


getUnprocessedMsfiles <- function(nameRawfilesIndex, screeningType) {
  wholeIndexHits <- getWholeIndex(indexName = nameRawfilesIndex)
  wholeIndexSource <- getSourceFromHits(hitsArray = wholeIndexHits, className = c("msrawfileRecord", "ntspRecord"))
  
  docsToProcess <- getDocsToProcess(wholeIndexSource, screeningType)
  docsInBatches <- splitDocsByDir(docsToProcess)
  docsInBatches
}

getWholeIndex <- function(indexName) {
  es_search_paged(escon, indexName, searchBody = list(
    query = list(match_all = stats::setNames(list(), character(0)))), 
    sort = "path")$hits$hits
}

getSourceFromHits <- function(hitsArray, className) {
  lapply(hitsArray, function(x) structure(x[["_source"]], class = className))
}

getDocsToProcess <- function(wholeIndexSource, screeningType) {
  fieldToCheck <- switch(screeningType, nts = "nts_last_eval", dbas = "dbas_last_eval", stop("screeningType unknown"))
  unprocessedDocs <- purrr::keep(wholeIndexSource, function(doc) !is.element(fieldToCheck, names(doc)))
  dirsWithUnprocessed <- unique(dirname(gf(unprocessedDocs, "path", character(1))))
  purrr::keep(wholeIndexSource, function(doc) dirname(doc$path) %in% dirsWithUnprocessed)
}

splitDocsByDir <- function(docsToProcess) {
  dirs <- dirname(gf(docsToProcess, "path", character(1)))
  split(docsToProcess, dirs)
}



