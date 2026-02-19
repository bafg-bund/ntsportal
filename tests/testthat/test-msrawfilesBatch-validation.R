


test_that("Batch check runs and returns error", {
  records <- getMsrawfilesTestRecords("dbas")
  recordsInBatches <- splitRecordsByDir(records)
  expect_error(
    suppressWarnings(checkQualityBatchList(recordsInBatches))
  )
})

test_that("Checking one batch for dba-screening runs without error", {
  records <- getMsrawfilesTestRecords("dbas")
  recordsInBatches <- recordsToBatches(records)
  checkResponse <- checkQualityBatch(recordsInBatches[[4]])
  expect_true(checkResponse)
})
