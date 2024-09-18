

test_that("nts index can be made", {
  source("~/connect-ntsp.R")
  indexName <- "ntsp_nts_test_index_temp"
  put_nts_index(escon, indexName)
  expect_true(elastic::index_exists(escon, indexName))
  mp1 <- elastic::index_get(escon, indexName)$ntsp_nts_test_index_temp$mappings$properties
  f <- fs::path_package("ntsportal", "extdata", "nts_index_mappings.json")
  mp2 <- jsonlite::read_json(f)$mappings$properties
  mp1 <- mp1[order(names(mp1))]
  mp2 <- mp2[order(names(mp2))]
  expect_true(all.equal(mp1, mp2))
  res <- elastic::index_delete(escon, indexName)
  expect_true(res$acknowledged)
})