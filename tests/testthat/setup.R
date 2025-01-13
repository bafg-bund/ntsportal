


# first run helper

recreateEntireTestMsrawfilesIndex <- function() {
  allRecords <- getAllMsrawfilesRecords(testIndexName)
  saveRDS(allRecords, test_path("fixtures", "screening-collectMsrawfileRecords", "entireTestMsrawfilesIndex.RDS"))
}


recreateAllMsrawfileRecords <- function() {
  allRecords <- getAllMsrawfilesRecords(testIndexName)
  allBatches <- splitRecordsByDir(allRecords)
  saveRDS(allBatches, test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
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