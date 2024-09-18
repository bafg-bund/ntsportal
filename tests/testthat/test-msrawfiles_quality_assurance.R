

test_that("Check nts", {
  docsList <- readRDS(test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList_error_batch.RDS"))
  expect_error(check_integrity_msrawfiles_nts(docsList))
  
  docsList <- readRDS(test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList.RDS"))
  expect_no_error(check_integrity_msrawfiles_nts(docsList))
  
  # source("~/connect-ntsp.R")
  # docsList <- get_msrawfiles(escon, "ntsp_index_msrawfiles_unit_tests")
  # saveRDS(docsList, test_path("fixtures", "msrawfiles_quality_assurance","whole_msrawfiles_docsList.RDS"))
})