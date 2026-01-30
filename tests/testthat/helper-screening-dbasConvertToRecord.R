


getFeatureRecordAndMsrawfileRecordNoSampleData <- function() {
  rr <- getOneSampleDbasResultAndRecords()
  featRecs <- convertToRecord(rr$dbasResult, rr$msrRecords)
  fieldsToRemove <- intersect(names(featRecs[[1]]), fieldsToMergeFromMsrawfiles())
  featRecs[[1]] <- removeFieldsFromRecord(featRecs[[1]], fieldsToRemove)
  rr$msrRecords <- nameRecordsByPath(rr$msrRecords)
  list(featRecs = featRecs, msrRecords = rr$msrRecords)
}

getOneSampleDbasResultAndRecords <- function() {
  compWithSpec <- "4-(2-Oxo-3-bornylidenemethyl)phenyl trimethylammonium"
  records <- getOneSampleRecords("dbas")
  result <- readRDS(test_path("fixtures", "screening-dbasConvertToRecord", "oneSampleDbasResult.RDS"))
  list(dbasResult = result, msrRecords = records, compWithSpec = compWithSpec)
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