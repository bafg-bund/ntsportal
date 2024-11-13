
connectNtsportal()

test_that("Batch check runs and returns error", {
  recordsInBatches <- readRDS(test_path("fixtures", "qualityAssuranceMsrawfiles", "recordsInBatches.RDS"))
  expect_error(
    suppressWarnings(checkQualityBatchList(recordsInBatches))
  )
})

test_that("Checking one batch for dbas processing runs without error", {
  recordsInBatches <- readRDS(test_path("fixtures", "qualityAssuranceMsrawfiles", "recordsInBatches.RDS"))
  listRecords <- recordsInBatches[[3]]
  checkResponse <- checkQualityBatch(listRecords)
  expect_true(checkResponse)
})


test_that("Check nts", {
  docsList <- readRDS(test_path("fixtures", "qualityAssuranceMsrawfiles","whole_msrawfiles_docsList_error_batch.RDS"))
  expect_error(check_integrity_msrawfiles_nts(docsList))
  
  docsList <- readRDS(test_path("fixtures", "qualityAssuranceMsrawfiles","whole_msrawfiles_docsList.RDS"))
  expect_no_error(check_integrity_msrawfiles_nts(docsList))
  
  # source("~/connect-ntsp.R")
  # docsList <- get_msrawfiles(escon, "ntsp_index_msrawfiles_unit_tests")
  # saveRDS(docsList, test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList.RDS"))
})


test_that("Quality check for records returns true", {
  index <- "ntsp_index_msrawfiles_unit_tests"
  records <- getAllMsrawfilesRecords(index)
  expect_true(validateAllMsrawfiles(records))
})

test_that("Quality check for records returns true", {
  index <- "ntsp_msrawfiles"
  records <- getAllMsrawfilesRecords(index)
  system.time(
    validateAllMsrawfiles(records)
  )
  expect_true(validateAllMsrawfiles(records))
})

test_that("Quality check for unit test index returns without error", {
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  rootPathRawFiles <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/"
  
  response <- check_integrity_msrawfiles(escon, rfindex = rfindex, locationRf = rootPathRawFiles)
  expect_true(response)
})
