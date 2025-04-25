

test_that("Batch check runs and returns error", {
  recordsInBatches <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "recordsInBatches.RDS"))
  expect_error(
    suppressWarnings(checkQualityBatchList(recordsInBatches))
  )
})

test_that("Checking one batch for dba-screening runs without error", {
  recordsInBatches <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "recordsInBatches.RDS"))
  listRecords <- recordsInBatches[[4]]
  checkResponse <- checkQualityBatch(listRecords)
  expect_true(checkResponse)
})

test_that("Normal record validation returns true", {
  records <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  expect_true(validateRecordsMsrawfiles(records))
})


test_that("An incorrect file path returns a warning", {
  records <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  records[[10]]$path <- "foobar"
  expect_warning(filesExist(records[[10]]))
  expect_warning(correctRawfileLocation(records[[10]]$path))
})

test_that("An missing path returns a warning", {
  records <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  records[[10]]$path <- NULL
  expect_warning(fieldsExistForSampleType(records[[10]]))
})

test_that("Wrong IS table returns a warning", {
  records <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  records[[10]]$pol <- "neg"
  expect_warning(correctIsTablePolarity(records[[10]]))
})

test_that("Wrong replicate regex returns a warning", {
  records <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  records[[10]]$dbas_replicate_regex <- "0[123]_pos"
  expect_warning(correctReplicateRegex(records[[10]]))
  
  records[[10]]$dbas_replicate_regex <- "0[567](_pos)"
  expect_warning(correctReplicateRegex(records[[10]]))
})

test_that("Wrong blank returns a warning", {
  records <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  records[[10]]$blank <- TRUE
  expect_warning(correctBlankRegex(records[[10]]))
})

test_that("An incorrect field in dbasRecord in a nested field results in an error", {
  oneSampleRecord <- getFeatureRecordAndMsrawfileRecord()$featureRecord[[1]]
  expect_true(validateRecord(oneSampleRecord))
  oneSampleRecord$ms2 <- c(oneSampleRecord$ms2, error_field = "error")
  expect_warning(validateRecord(oneSampleRecord))
  expect_false(suppressWarnings(validateRecord(oneSampleRecord)))
})

