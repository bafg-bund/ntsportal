
test_that("You can retrieve more than 10000 results", {
  indexName <- "ntsp_msrawfiles"
  hits <- esSearchPaged(indexName, sort = "path", source = "path")$hits$hits
  expect_gt(length(hits), 10000)
})
