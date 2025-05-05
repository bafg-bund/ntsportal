

test_that("You can compute the linear regression for one compound", {
  records <- computeLinearRegression("ntsp25.1_dbas_upb", queryBlock = list(term = list(name = "Sitagliptin")))
  tb <- convertRecordsToTibble(records)
  expect_gte(nrow(tb), 2)
  expect_equal(tb[1, "name", drop = T], "Sitagliptin")
  #updateLinearRegressionTable("ntsp25.1_dbas_upb")
})
