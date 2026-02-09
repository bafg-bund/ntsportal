

getSelectedMsrawfileBatches <- function(msrawfilesIndex, rootDirs, screeningType) {
  warnNonExistentDirs(rootDirs)
  batchNames <- getAllBatchesInDir(msrawfilesIndex, rootDirs)
  if (length(batchNames) == 0)
    stop("No batches found in dir ", paste(rootDirs, collapse = ", "))
  allRecords <- getTableAsRecords(
    getDbComm(), msrawfilesIndex, 
    searchBlock = list(query = list(terms = list(batchname = as.list(batchNames)))), sortField = "start", 
    fields = msrawfilesFieldsForProcessing(screeningType),
    recordConstructor = switch(screeningType, dbas = newDbasMsrawfilesRecord, nts = newNtsMsrawfilesRecord)
  )
  recordsToBatches(allRecords)
}

recordsToBatches <- function(records) {
  groupedRecords <- splitRecordsByDir(records)
  groupedRecords <- map(groupedRecords, orderByStart)
  map(groupedRecords, recordsToOneBatch)
}

orderByStart <- function(records) {
  recordsWithStart <- keep(records, \(x) "start" %in% names(x))
  recordsWithoutStart <- discard(records, \(x) "start" %in% names(x))
  if (length(recordsWithStart) > 0) {
    startTimes <- map_chr(recordsWithStart, \(x) x$start)
    stopifnot(all(grepl("\\d{4}-\\d{2}-\\d{2}", startTimes)))
    startTimes <- lubridate::ymd(startTimes)
    recordsWithStart <- recordsWithStart[order(startTimes)]
  }
  c(recordsWithStart, recordsWithoutStart)
}

recordsToOneBatch <- function(records) {
  recordType <- class(records[[1]])[1]
  batchMaker <- switch(recordType, 
                       dbasMsrawfilesRecord = newDbasMsrawfilesBatch, 
                       ntsMsrawfilesRecord = newNtsMsrawfilesBatch)
  batchMaker(records)
} 

getAllBatchesInDir <- function(msrawfilesIndex, rootDirs) {
  dirsToKeep <- list_c(map(rootDirs, \(x) normalizePath(list.dirs(x))))
  allBatches <- getUniqueValues(getDbComm(), msrawfilesIndex, "batchname", maxLength = 100000)
  warnMissingBatches(dirsToKeep, allBatches)
  dirsToKeep[dirsToKeep %in% allBatches]
}

warnMissingBatches <- function(dirsToKeep, allBatches) {
  missingBatches <- dirsToKeep[!(dirsToKeep %in% allBatches)]
  if (length(missingBatches) > 0) {
    warning("No batches found for directories: ", paste(missingBatches, collapse = ", "))
  }
}

warnNonExistentDirs <- function(rootDirs) {
  if (!all(dir.exists(rootDirs))) {
    badDirs <- purrr::discard(rootDirs, dir.exists)
    warning("The following directories do not exist: ", paste(badDirs, collapse = ", "))
  }
}

msrawfilesFieldsForProcessingGeneral <- function() {
  c(
    "blank",
    "chrom_method",
    "csl_instruments_allowed",
    "feature_table_alias",
    "internal_standard",
    "path",
    "pol",
    "replicate_regex",
    "spectral_library_path",
    "start"
  )
}

msrawfilesFieldsForProcessing <- function(screeningType) {
  allFieldNames <- names(getMappingProperties("msrawfiles"))
  processingFields <- allFieldNames[grepl(glue("^{screeningType}_"), allFieldNames)]
  c(processingFields, msrawfilesFieldsForProcessingGeneral())
}

#' @export
newMsrawfilesBatch <- function(msrawfilesRecords = list(), ..., class = character()) {
  stopifnot(is.list(msrawfilesRecords))
  stopifnot(all(map_lgl(msrawfilesRecords, \(rec) inherits(rec, "msrawfilesRecord"))))
  structure(msrawfilesRecords, ..., class = c(class, "msrawfilesBatch", "list"))
}
#' @export
newDbasMsrawfilesBatch <- function(msrawfilesRecords = list()) {
  stopifnot(is.list(msrawfilesRecords))
  stopifnot(all(map_lgl(msrawfilesRecords, \(rec) inherits(rec, "dbasMsrawfilesRecord"))))
  newMsrawfilesBatch(msrawfilesRecords, class = "dbasMsrawfilesBatch")
}
#' @export
newNtsMsrawfilesBatch <- function(msrawfilesRecords = list()) {
  stopifnot(is.list(msrawfilesRecords))
  stopifnot(all(map_lgl(msrawfilesRecords, \(rec) inherits(rec, "ntsMsrawfilesRecord"))))
  newMsrawfilesBatch(msrawfilesRecords, class = "ntsMsrawfilesBatch")
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
