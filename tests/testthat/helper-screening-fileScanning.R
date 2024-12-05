

getRecordsTripicateBatch <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  allBatches[[4]]
}

getRecordsSampleAndBlank <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  allBatches[[1]][3:4]
}

getMergedReportSampleAndBlank <- function() {
  readRDS(test_path("fixtures", "screening-fileScanning", "mergedReportSampleAndBlank.RDS"))
}

getRecordNoPeaks <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  allBatches[[2]][[1]]
}

getSingleRecordDes_07_01_pos <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  allBatches[[4]][[3]]
}

getReportReplicateBatch <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  report <- readRDS(test_path("fixtures", "screening-fileScanning", "blankCorrectedReportTriplicate.RDS"))
  list(report = report, records = allBatches[[4]])
}

