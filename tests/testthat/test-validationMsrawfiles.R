
connectNtsportal()

test_that("Batch check runs and returns error", {
  recordsInBatches <- readRDS(test_path("fixtures", "validationMsrawfiles", "recordsInBatches.RDS"))
  expect_error(
    suppressWarnings(checkQualityBatchList(recordsInBatches))
  )
})

test_that("Checking one batch for dbas processing runs without error", {
  recordsInBatches <- readRDS(test_path("fixtures", "validationMsrawfiles", "recordsInBatches.RDS"))
  listRecords <- recordsInBatches[[3]]
  checkResponse <- checkQualityBatch(listRecords)
  expect_true(checkResponse)
})

test_that("Normal record validation returns true", {
  records <- readRDS(test_path("fixtures", "validationMsrawfiles", "allRecordsFlat.RDS"))
  expect_true(validateRecordsMsrawfiles(records))
})


test_that("An incorrect file path returns a warning", {
  records <- readRDS(test_path("fixtures", "validationMsrawfiles", "allRecordsFlat.RDS"))
  records[[10]]$path <- "foobar"
  expect_warning(filesExist(records[[10]]))
})



