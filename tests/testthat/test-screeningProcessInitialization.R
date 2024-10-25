

test_that("Unprocessed files can be collected from msrawfiles", {
  
  unprocessedMsFiles <- getUnprocessedMsfiles(nameRawfilesIndex = "ntsp_index_msrawfiles_unit_tests", screeningType = "nts")
  
  expect_s3_class(unprocessedMsFiles[[1]][[1]], c("msrawfileRecord", "ntspRecord"))
  expect_equal(length(unprocessedMsFiles), 4)
  
})