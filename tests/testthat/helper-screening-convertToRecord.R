



getOneSampleDbasResultAndRecords <- function() {
  records <- getOneSampleRecords()
  result <- readRDS(test_path("fixtures", "screening-convertToRecord", "oneSampleDbasResult.RDS"))
  list(dbasResult = result, records = records)
}

getOneSampleRecords <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  allBatches[[1]][3:4]
}

getFeaturesAndRecordsNoSampleData <- function() {
  records <- getOneSampleRecords()
  records <-nameRecordsByFilename(records)
  features <- readRDS(test_path("fixtures", "screening-convertToRecord", "featuresNoSampleData.RDS"))
  list(features = features, records = records)
}

getOneFeature <- function() {
  readRDS(test_path("fixtures", "screening-convertToRecord", "featureNoSpectra.RDS"))
}

getMsrawfileRecordNoPeaks <- function() {
  allBatches <- readRDS(test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
  allBatches[[2]][[1]]
}