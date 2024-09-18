

test_that("Add new values (each length 1, given as ...) to docs", {
  source("~/connect-ntsp.R")
  
  es_add_value(
    escon, "ntsp_index_msrawfiles_unit_tests", 
    queryBody = list(term = list(filename = "RH_pos_20220602_no_peaks.mzXML")),
    river = "testing_change_back_to_rhein",
    gkz = 9999
  )
  Sys.sleep(1)
  res <- elastic::Search(
    escon, "ntsp_index_msrawfiles_unit_tests", 
    body = list(
      query = list(term = list(filename = "RH_pos_20220602_no_peaks.mzXML"))
    )
  )$hits$hits[[1]][["_source"]]
  
  expect_equal(res$river, "testing_change_back_to_rhein")
  expect_equal(res$gkz, 9999)
  
  es_add_value(
    escon, "ntsp_index_msrawfiles_unit_tests", 
    queryBody = list(term = list(filename = "RH_pos_20220602_no_peaks.mzXML")),
    river = "rhein",
    gkz = 2
  )
  Sys.sleep(1)
})


test_that("Add new values (each length 1, given as list) to docs", {
  source("~/connect-ntsp.R")
  
  es_add_value(
    escon, "ntsp_index_msrawfiles_unit_tests", 
    queryBody = list(term = list(filename = "RH_pos_20220602_no_peaks.mzXML")),
    listToAdd = list(river = "testing_change_back_to_rhein", gkz = 9999)
  )
  Sys.sleep(1)
  res <- elastic::Search(
    escon, "ntsp_index_msrawfiles_unit_tests", 
    body = list(
      query = list(term = list(filename = "RH_pos_20220602_no_peaks.mzXML"))
    )
  )$hits$hits[[1]][["_source"]]
  
  expect_equal(res$river, "testing_change_back_to_rhein")
  expect_equal(res$gkz, 9999)
  
  es_add_value(
    escon, "ntsp_index_msrawfiles_unit_tests", 
    queryBody = list(term = list(filename = "RH_pos_20220602_no_peaks.mzXML")),
    river = "rhein",
    gkz = 2
  )
  Sys.sleep(1)
})