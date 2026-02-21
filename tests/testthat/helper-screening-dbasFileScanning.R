
getRecordsTripicateBatch <- function() {
  records <- getMsrawfilesTestRecords("dbas")
  allBatches <- splitRecordsByDir(records)
  names(allBatches)
  allBatches[[grep("unit_tests/olmesartan-d6-bisoprolol", names(allBatches))]]
}

getMergedReportSampleAndBlank <- function() {
  readRDS(test_path("fixtures", "screening-dbasFileScanning", "mergedReportSampleAndBlank.RDS"))
}

getRecordsDessauBatch <- function() {
  readRDS(test_path("fixtures", "screening-dbasFileScanning", "recordsDessauBatch.RDS"))
}

getReintegratedReportDessauBatch <- function() {
  readRDS(test_path("fixtures", "screening-dbasFileScanning", "reintegratedReportDessauBatch.RDS"))
}


getReportReplicateBatch <- function() {
  records <- getMsrawfilesTestRecords("dbas")
  allBatches <- splitRecordsByDir(records)
  recs <- allBatches[[grep("unit_tests/olmesartan-d6-bisoprolol", names(allBatches))]]
  recsBatch <- newDbasMsrawfilesBatch(recs)
  report <- readRDS(test_path("fixtures", "screening-dbasFileScanning", "blankCorrectedReportTriplicate.RDS"))
  list(report = report, records = recsBatch)
}
