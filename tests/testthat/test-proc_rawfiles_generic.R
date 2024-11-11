
connectNtsportal()


test_that("New indexes can be created for nts processing", {
  source("~/connect-ntsp.R")
  
  create_index_all(escon, rfindex = "ntsp_index_msrawfiles_unit_tests", type = "nts", dateNum = "240905")
  
  expect_true(
    elastic::index_exists(escon, "ntsp_index_nts_v240905_unit_tests")
  )
  res <- elastic::index_delete(escon, index = "ntsp_index_nts_v240905_unit_tests")
  expect_true(res$acknowledged)
  
})

