

getFeatureRecordAndMsrawfileRecordDbas <- function() {
  rr <- getOneSampleDbasResultAndRecords()
  featureRecord <- convertToRecord(rr$dbasResult, rr$records)
  list(featureRecord = featureRecord, msrawfileRecord = rr$records)
}

getOneSampleDbasResultAndRecords <- function() {
  records <- getOneSampleRecords("dbas")
  result <- readRDS(test_path("fixtures", "screening-dbasConvertToRecord", "oneSampleDbasResult.RDS"))
  list(dbasResult = result, records = records)
}



getFeatureRecordAndMsrawfileRecordNoSampleData <- function() {
  rr <- getFeatureRecordAndMsrawfileRecordDbas()
  fieldsToRemove <- intersect(names(rr$featureRecord[[1]]), fieldsToMergeFromMsrawfiles())
  rr$featureRecord[[1]] <- removeFieldsFromRecord(rr$featureRecord[[1]], fieldsToRemove)
  rr$msrawfileRecord <- nameRecordsByPath(rr$msrawfileRecord)
  list(featureRecord = rr$featureRecord, msrawfileRecord = rr$msrawfileRecord)
}

getFeatureRecordAndMsrawfileRecordNoSpectra <- function() {
  rr <- getFeatureRecordAndMsrawfileRecordDbas()
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
getDbasScanResultDuplicatePeaks <- function() {
  fldr <- file.path("fixtures", "screening-dbasConvertToRecord")
  readRDS(test_path(fldr, "scanResultOneSampleWithDuplicatePeaks.RDS"))
}
getDbasBatchDuplicatePeaks <- function() {
  fldr <- file.path("fixtures", "screening-dbasConvertToRecord")
  readRDS(test_path(fldr, "msrawfilesBatchOneSampleWithDuplicatePeaks.RDS"))
}
dbasScanResultWithMultiHitGapFilledPeaks <- function() {
  msrBatch <- getDbasBatchDuplicatePeaks()
  msrBatch <- msrBatch[c(1,1)]
  msrBatch[[2]]$path <- "/blah/foobar.mzXML"
  msrBatch <- newDbasMsrawfilesBatch(msrBatch)
  sr <- getDbasScanResultDuplicatePeaks()
  sr$reintegrationResults <- sr$reint[c(1,2,3,1,2),]
  sr$reintegrationResults[c(4,5), "samp"] <-  "foobar.mzXML"
  sr$rawFilePaths <- c(sr$raw[1], "/blah/foobar.mzXML") 
  list(scanResult = sr, batch = msrBatch)
}