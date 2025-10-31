

test_that("A template record can be found", {
  testId <- findTemplateId(testIndexName, blank = TRUE, matrix = "water", duration = "P1D")
  expect_equal(nchar(testId), 20)
})
