
test_that("You can convert an ntsResult to a list of features", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  startTime <- Sys.time()
  features <- convertToRecord(ntsResult)
  endTime <- Sys.time()
  message("Time needed to convert sample and blank to features: ", 
          round(difftime(endTime, startTime, units = "secs")), " s") 
  expect_length(features, 27)
  expect_contains(unique(list_c(map(features, names))), c("ms1", "ms2", "intensity", "internal_standard", 
                                                          "feature_table_alias"))
  expect_null(names(features))
  expect_false(any(is.element(unique(list_c(map(features, names))), c("sampleId", "peakId", "componentId", 
                                                                      "alignmentId", "ms2scan"))))
  expect_s3_class(features[[1]], "featureRecord")
})

test_that("You can remove annotations from an ntsResult", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultBisoprololBatch.RDS"))
  ntsResultNew <- removeAnnotated(ntsResult)
  expect_lt(nrow(ntsResultNew$alignmentTable), nrow(ntsResult$alignmentTable))
  
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsScanResult-ntsRealBatchRheinKobPos.RDS"))
  ntsResultNew <- removeAnnotated(ntsResult)
  expect_lt(nrow(ntsResultNew$alignmentTable), nrow(ntsResult$alignmentTable))
})


test_that("You can remove blank files from an ntsResult", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  newNtsResult <- removeBlankFiles(ntsResult)
  expect_s3_class(newNtsResult, "ntsResult")
  expect_lt(nrow(newNtsResult$peakList), nrow(ntsResult$peakList))
  expect_lt(nrow(newNtsResult$alignmentTable), nrow(ntsResult$alignmentTable))
  expect_lt(nrow(newNtsResult$sampleList), nrow(ntsResult$sampleList))
})

test_that("An initial list of featureRecords can be made", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  testFeatures <- getAreasOfFeatures(ntsResult)
  expect_length(testFeatures, 28)
  expect_contains(names(testFeatures[[1]]), "mz")
})

test_that("You can add int std intensities and areas to features", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  testFeatures <- getAreasOfFeatures(ntsResult)
  featWithIntStd <- addIntStdToFeatures(ntsResult, testFeatures)
  expect_contains(names(featWithIntStd[[1]]), c("internal_standard", "area_internal_standard"))
  expect_equal(featWithIntStd[[1]]$internal_standard, "Olmesartan-d6")
  expect_gt(featWithIntStd[[1]]$area_internal_standard, featWithIntStd[[1]]$intensity_internal_standard)
})

test_that("You can add the alias name to records", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  testFeatures <- getAreasOfFeatures(ntsResult)
  featWithAlias <- addAliasToFeatures(ntsResult, testFeatures)
  expect_contains(names(featWithAlias[[1]]), "feature_table_alias")
})

test_that("You can open measurement file and test that it is open, then extract an EIC, MS1, MS2", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  testFeatures <- getAreasOfFeatures(ntsResult)
  measFile <- openMeasFile(testFeatures[[1]]$path)
  peakId <- testFeatures[[1]]$peakId
  expect_true(isMeasFile(measFile, testFeatures[[1]]))
  
  testEic <- getEicFromScanResult(ntsResult, peakId, measFile = measFile, rec = testFeatures[[1]])
  expect_contains(names(testEic[[1]]), c("time", "int"))
  expect_length(testEic, 46)
  expect_type(testEic, "list")
  expect_type(testEic[[1]], "list")
  expect_type(testEic[[1]]$time, "double")
  
  testMs1 <- getMs1FromScanResult(ntsResult, peakId, measFile = measFile, rec = testFeatures[[1]])
  expect_contains(names(testMs1[[1]]), c("mz", "int"))
  expect_length(testMs1, 34)
  expect_type(testMs1, "list")
  expect_type(testMs1[[1]], "list")
  expect_type(testMs1[[1]]$mz, "double")
  
  testMs2Empty <- getMs2FromScanResult(ntsResult, peakId, measFile = measFile, rec = testFeatures[[1]])
  expect_null(testMs2Empty)
  firstWithMs2 <- which(sapply(testFeatures, \(x) x$ms2scan) != 0)[1]
  peakId <- testFeatures[[firstWithMs2]]$peakId
  testMs2 <- getMs2FromScanResult(ntsResult, peakId, measFile = measFile, rec = testFeatures[[5]])
  expect_contains(names(testMs2[[1]]), c("mz", "int"))
  expect_length(testMs2, 50)
  expect_type(testMs2, "list")
  expect_type(testMs2[[1]], "list")
  expect_type(testMs2[[1]]$mz, "double")
  expect_equal(testMs2[[1]]$int, 1)
})

test_that("A failed EIC extraction adds nothing to a record", {
  ntsResult <- readRDS(test_path("fixtures", "screening-nts", "ntsResultSampleAndBlank.RDS"))
  testFeatures <- getAreasOfFeatures(ntsResult)
  testRec <- list()
  testRec$eic <- with_log_threshold(getEicFromScanResult(ntsResult, 1, measFile = "foobar", rec = testFeatures[[1]]), OFF)
  expect_length(testRec, 0)
})

test_that("CleanUpMs2 returns an empty ms2 if no peaks remain", {
  ms2 <- tibble(mz = 900, int = 1)
  rec = list(mz = 100)
  res <- cleanUpMs2(ms2, rec)
  expect_s3_class(res, "tbl_df")
  expect_equal(nrow(res), 0)
})
