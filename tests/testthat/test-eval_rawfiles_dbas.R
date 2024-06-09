test_that("Test file KO_06_1_pos.mzXML can be processed for CBZ", {
  logger::with_log_threshold(
    source("~/connect-ntsp.R"),
    threshold = "OFF"
  )
  
  re <- proc_esid(
    escon = escon,
    rfindex = "ntsp_index_msrawfiles_unit_tests",
    esid = "dd8ohI4BbGGk3ENz_S5y",
    compsProcess = "Carbamazepine"
  )
  expect_contains(re$peakList$comp_name, "Carbamazepine")
})