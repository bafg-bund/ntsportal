


test_that("A small result can be reformated to a dbasRecord object", {
  resultAndRecords <- getOneSampleDbasResultAndRecords()
  oneSampleRecord <- convertToRecord(resultAndRecords$dbasResult, resultAndRecords$records)
  expect_contains(names(oneSampleRecord[[1]]), c("mz", "rt", "intensity", "ms2", "dbas_alias_name"))
  expect_s3_class(oneSampleRecord[[1]], "featureRecord")
  expect_true(validateRecord(oneSampleRecord[[1]]))
})

test_that("Sample data is added to features", {
  featuresAndReport <- getFeaturesAndRecordsNoSampleData()
  featureWithData <- addSampleData(featuresAndReport[[1]], featuresAndReport[[2]])
  expect_contains(names(featureWithData[[1]]), c("river", "loc", "station"))
  expect_true(validateRecord(newFeatureRecord(featureWithData[[1]])))
})

test_that("Record is reduced to selected fields", {
  fields <- fieldsToMergeFromMsrawfiles()
  records <- getOneSampleRecords()
  recordReduced <- reduceRecordToFields(records[[1]], fields)
  expect_equal(length(recordReduced), 14)
})


test_that("Spectra area added to feature", {
  scanResult <- getOneSampleDbasResultAndRecords()[["dbasResult"]]
  feature <- getOneFeature()
  feature2 <- addSpectraToFeature(feature, scanResult)
  expect_contains(names(feature2), "ms2")
})


test_that("An empty result is converted to an empty record", {
  emptyResult <- convertToDbasResult(emptyReport())
  record <- convertToRecord(emptyResult, list(getMsrawfileRecordNoPeaks()))
  expect_equal(length(record), 1)
  expect_s3_class(record[[1]], "featureRecord")
  expect_match(record[[1]]$path,"^/srv/.*\\.mzX?ML$")
})
