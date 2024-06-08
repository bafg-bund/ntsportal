test_that("Index name created from alias", {
  expect_equal(dbas_index_from_alias("ntsp_dbas_bfg", dateNum = "240607"), "ntsp_index_dbas_v240607_bfg")
})
