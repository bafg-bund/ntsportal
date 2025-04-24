# first run helper

# Create new msrawfiles_unit_tests index
dbComm <- PythonDbComm()
copyTable(dbComm, "ntsp_msrawfiles_unit_tests", testIndexName, "msrawfiles")
changeAllDbasAliasNames(dbComm, testIndexName, ntspVersion)

# deleteIndex(testIndexName)

recreateEntireTestMsrawfilesIndex <- function() {
  allRecords <- getAllMsrawfilesRecords(testIndexName)
  saveRDS(allRecords, test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
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

