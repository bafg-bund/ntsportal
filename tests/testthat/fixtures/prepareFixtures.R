# first run helper

# Create new msrawfiles_unit_tests index
createNewIndex(testIndexName, "msrawfiles")
reindexDocs("ntsp_msrawfiles_unit_tests", testIndexName)
updateAllDocs(testIndexName, script=glue("ctx._source.dbas_alias_name = 'ntsp{ntspVersion}_dbas_unit_tests'"))

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

