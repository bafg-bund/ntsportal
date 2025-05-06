
test_that("You can retrieve more than 10000 results", {
  indexName <- "ntsp_msrawfiles"
  hits <- esSearchPaged(indexName, sort = "path", source = "path")$hits$hits
  expect_gt(length(hits), 10000)
})

test_that("Connection can be tested", {
  expect_no_error(testConnection())
})

test_that("Connection fails", {
  expect_error(
    withr::with_options(
      list(ntsportal.elasticsearchHostUrl = "blah"), 
      testConnection()
    )
  )
})
