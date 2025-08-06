

getFeatureRecord <- function() {
  getFeatureRecordAndMsrawfileRecord()$featureRecord
}

getFeatureRecordAndMsrawfileRecord <- function() {
  rr <- getOneSampleDbasResultAndRecords()
  #debugonce(getAreasOfFeatures)
  featureRecord <- convertToRecord(rr$dbasResult, rr$records)
  list(featureRecord = featureRecord, msrawfileRecord = rr$records)
}

getOneSampleDbasResultAndRecords <- function() {
  records <- getOneSampleRecords()
  result <- readRDS(test_path("fixtures", "screening-convertToRecord", "oneSampleDbasResult.RDS"))
  list(dbasResult = result, records = records)
}



getFeatureRecordAndMsrawfileRecordNoSampleData <- function() {
  rr <- getFeatureRecordAndMsrawfileRecord()
  fieldsToRemove <- intersect(names(rr$featureRecord[[1]]), fieldsToMergeFromMsrawfiles())
  rr$featureRecord[[1]] <- removeFieldsFromRecord(rr$featureRecord[[1]], fieldsToRemove)
  rr$msrawfileRecord <- nameRecordsByPath(rr$msrawfileRecord)
  list(featureRecord = rr$featureRecord, msrawfileRecord = rr$msrawfileRecord)
}

getFeatureRecordAndMsrawfileRecordNoSpectra <- function() {
  rr <- getFeatureRecordAndMsrawfileRecord()
  fieldsToRemove <- c("ms1", "ms2", "eic")
  rr$featureRecord[[1]] <- removeFieldsFromRecord(rr$featureRecord[[1]], fieldsToRemove)
  rr$msrawfileRecord <- nameRecordsByPath(rr$msrawfileRecord)
  list(featureRecord = rr$featureRecord, msrawfileRecord = rr$msrawfileRecord)
}

removeFieldsFromRecord <- function(rec, fieldsToRemove) {
  for (field in fieldsToRemove) {
    rec[[field]] <- NULL
  }
  rec
}
