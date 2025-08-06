

test_that("A template record can be found", {
  testId <- findTemplateId(testIndexName, blank = TRUE, matrix = "water", duration = 1)
  expect_equal(nchar(testId), 20)
})
