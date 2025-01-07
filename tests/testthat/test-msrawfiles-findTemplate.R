
connectNtsportal()

test_that("A template record can be found", {
  indexName <- "ntsp_index_msrawfiles_unit_tests"
  testId <- findTemplateId(indexName, blank = TRUE, matrix = "water", duration = 1)
  expect_equal(nchar(testId), 20)
})
