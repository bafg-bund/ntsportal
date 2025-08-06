

test_that("A small result can be reformated to a dbasRecord object", {
  featureRecord <- getFeatureRecord()
  expect_contains(names(featureRecord[[1]]), c("mz", "rt", "intensity", "ms2", "dbas_alias_name", "csl_experiment_id"))
  expect_s3_class(featureRecord[[1]], "featureRecord")
  expect_true(validateRecord(featureRecord[[1]]))
  differenceIntArea <- featureRecord[[1]]$area - featureRecord[[1]]$intensity
  expect_gt(differenceIntArea, 100)
  checkForAlias(featureRecord[[1]])
})

test_that("Sample data is added to features", {
  featureRecMsrawfileRec <- getFeatureRecordAndMsrawfileRecordNoSampleData()
  featureWithData <- addSampleInfo(featureRecMsrawfileRec$featureRecord, featureRecMsrawfileRec$msrawfileRecord)
  expect_contains(names(featureWithData[[1]]), c("path"))
  expect_true(validateRecord(newFeatureRecord(featureWithData[[1]])))
})

test_that("Record is reduced to selected fields", {
  fields <- fieldsToMergeFromMsrawfiles()
  records <- getOneSampleRecords()
  recordReduced <- reduceRecordToFields(records[[1]], fields)
  expect_equal(length(recordReduced), 1)
  expect_contains(names(recordReduced), "dbas_alias_name")
})

test_that("Spectra area added to feature", {
  scanResult <- getOneSampleDbasResultAndRecords()$dbasResult
  feature <- getFeatureRecordAndMsrawfileRecordNoSpectra()$featureRecord[[1]]
  feature2 <- addSpectraToFeature(feature, scanResult)
  expect_contains(names(feature2), "ms2")
  expect_contains(names(feature2), "score_ms2_match")
})

test_that("MS2 matching score can be read for a peak ID", {
  scanResult <- getOneSampleDbasResultAndRecords()$dbasResult
  matchScore <- getScoreMs2Match(6, scanResult)
  expect_gt(matchScore, 300)
  expect_true(is.integer(matchScore))
})

test_that("An empty result is converted to an empty record", {
  record <- getEmptyRecord()
  expect_equal(length(record), 1)
  expect_s3_class(record[[1]], "featureRecord")
  expect_match(record[[1]]$path, "msrawfiles/unit_tests/.*\\.mzX?ML$")
  expect_match(record[[1]]$dbas_alias_name,"^ntsp")
})

test_that("Internal standard name, intensity and area is added to record", {
  resultAndRecords <- getOneSampleDbasResultAndRecords()
  testRecordList <- convertToRecord(resultAndRecords$dbasResult, resultAndRecords$records)
  
  expect_contains(names(testRecordList[[1]]), "internal_standard")
  expect_equal(testRecordList[[1]]$internal_standard, "Olmesartan-d6")
  expect_contains(names(testRecordList[[1]]), "area_internal_standard")
  expect_contains(names(testRecordList[[1]]), "intensity_internal_standard")
})

test_that("If the internal standard is not found, there is no addition of area and intensity to the doc", {
  resultAndRecords <- getOneSampleDbasResultAndRecords()
  resultAndRecords$records[[1]]$dbas_is_name <- "Foobar"
  testRecordList <- convertToRecord(resultAndRecords$dbasResult, resultAndRecords$records)
  
  expect_contains(names(testRecordList[[1]]), "internal_standard")
  expect_equal(testRecordList[[1]]$internal_standard, "Foobar")
  expect_false(is.element("area_internal_standard", names(testRecordList[[1]])))
})
