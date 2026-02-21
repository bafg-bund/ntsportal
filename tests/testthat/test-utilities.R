
test_that("You can retrieve more than 10000 results", {
  indexName <- "ntsp25.3_msrawfiles"
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

test_that("You can create tables of all mappings", {
  tableTypes <- getNtsportalTableTypes()
  expect_no_error(map(tableTypes, makeMappingsTbl))
})

test_that("splitRecordsByDir works with length 0 input", {
  records <- splitRecordsByDir(list())
  expect_length(records, 0)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
