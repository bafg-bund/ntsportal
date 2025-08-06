



getRecordsTripicateBatch <- function() {
  records <- getMsrawfilesTestRecords()
  allBatches <- splitRecordsByDir(records)
  names(allBatches)
  allBatches[[grep("unit_tests/olmesartan-d6-bisoprolol", names(allBatches))]]
}

getRecordsSampleAndBlank <- function() {
  getOneSampleRecords()
}

getMergedReportSampleAndBlank <- function() {
  readRDS(test_path("fixtures", "screening-fileScanning", "mergedReportSampleAndBlank.RDS"))
}

getMergedReportTriplicateBatch <- function() {
  readRDS(test_path("fixtures", "screening-fileScanning", "mergedReportTriplicateBatch.RDS"))
}

getRecordsDessauBatch <- function() {
  readRDS(test_path("fixtures", "screening-fileScanning", "recordsDessauBatch.RDS"))
}
getMergedReportDessauBatch <- function() {
  readRDS(test_path("fixtures", "screening-fileScanning", "mergedReportDessauBatch.RDS"))
}



getSingleRecordDes_07_01_pos <- function() {
  records <- getMsrawfilesTestRecords()
  keep(records, \(rec) grepl("Des_07_01_pos", rec$path))
}

getReportReplicateBatch <- function() {
  records <- getMsrawfilesTestRecords()
  allBatches <- splitRecordsByDir(records)
  report <- readRDS(test_path("fixtures", "screening-fileScanning", "blankCorrectedReportTriplicate.RDS"))
  list(report = report, records = allBatches[[grep("unit_tests/olmesartan-d6-bisoprolol", names(allBatches))]])
}

