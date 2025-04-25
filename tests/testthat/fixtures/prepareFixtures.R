# first run helper

# Create new msrawfiles_unit_tests index
createNewMsrawfileUnitTestsIndex <- function() {
  dbComm <- getOption("ntsportal.dbComm")()
  copyTable(dbComm, "ntsp_msrawfiles_unit_tests", testIndexName, "msrawfiles")
  changeAllDbasAliasNames(dbComm, testIndexName, ntspVersion)
}


# deleteIndex(testIndexName)

recreateEntireTestMsrawfilesIndex <- function() {
  dbComm <- getOption("ntsportal.dbComm")()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  saveRDS(allRecords, test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
}

buildMsrawfilesAllRecords <- function() {
  dbComm <- getOption("ntsportal.dbComm")()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  saveRDS(allRecords, test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
}

buildRecordsInBatches <- function() {
  dbComm <- getOption("ntsportal.dbComm")()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  recordsInBatches <- splitRecordsByDir(allRecords)
  saveRDS(recordsInBatches, test_path("fixtures", "msrawfilesTestRecords", "recordsInBatches.RDS"))
}

buildAllRecordsFlat <- function() {
  dbComm <- getOption("ntsportal.dbComm")()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
}

recreateReportForCleaning <- function() {
  reports <- purrr::map(getRecordsTripicateBatch(), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-fileScanning", "reportForCleaning.RDS"))
}

recreateMergedReportSampleAndBlank <- function() {
  reports <- purrr::map(getRecordsSampleAndBlank(), fileScanDbas)
  reports <- removeEmptyReports(reports)
  mergedReport <- mergeReports(reports)
  saveRDS(mergedReport, test_path("fixtures", "screening-fileScanning", "mergedReportSampleAndBlank.RDS"))
}

