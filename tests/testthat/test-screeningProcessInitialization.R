
connectNtsportal()
test_that("Unprocessed files can be collected from msrawfiles", {
  index <- "ntsp_index_msrawfiles_unit_tests"
  unprocessedMsFiles <- getUnprocessedMsrawfilesRecords(nameMsrawfilesIndex = index, screeningType = "nts")
  
  expect_s3_class(unprocessedMsFiles[[1]][[1]], "msrawfileRecord")
  expect_equal(length(unprocessedMsFiles), 4)
  
})
