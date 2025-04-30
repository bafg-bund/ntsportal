# first run helper

# Create new msrawfiles_unit_tests index
createNewMsrawfileUnitTestsIndex <- function() {
  dbComm <- getDbComm()
  copyTable(dbComm, "ntsp_msrawfiles_unit_tests", testIndexName, "msrawfiles")
  changeAllDbasAliasNames(dbComm, testIndexName, ntspVersion)
}


# deleteIndex(testIndexName)
buildMsrawfilesAllRecords <- function() {
  dbComm <- getDbComm()
  allRecords <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  saveRDS(allRecords, test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
}

buildOneSampleDbasResult <- function() {
  records <- getOneSampleRecords()
  result <- scanBatchDbas(records)
  saveRDS(result, test_path("fixtures", "screening-convertToRecord", "oneSampleDbasResult.RDS"))
}



buildAllRecordsFlat <- function() {
  dbComm <- getDbComm()
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

