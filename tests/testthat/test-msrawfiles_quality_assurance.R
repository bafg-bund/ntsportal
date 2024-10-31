
connectNtsportal()

test_that("Check nts", {
  docsList <- readRDS(test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList_error_batch.RDS"))
  expect_error(check_integrity_msrawfiles_nts(docsList))
  
  docsList <- readRDS(test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList.RDS"))
  expect_no_error(check_integrity_msrawfiles_nts(docsList))
  
  # source("~/connect-ntsp.R")
  # docsList <- get_msrawfiles(escon, "ntsp_index_msrawfiles_unit_tests")
  # saveRDS(docsList, test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList.RDS"))
})



test_that("Quality check for unit test index returns without error", {
  rfindex <- "ntsp_index_msrawfiles_unit_tests"
  rootPathRawFiles <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/"
  
  response <- check_integrity_msrawfiles(escon, rfindex = rfindex, locationRf = rootPathRawFiles)
  expect_true(response)
})