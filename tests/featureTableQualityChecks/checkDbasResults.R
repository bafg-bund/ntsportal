library(ntsportal)
library(testthat)

tableName <- "ntsp26.1_feature*"
tableName <- "ntsp26.1_index_feature_v260918151755_upb"
test_that("All batches have at least 2 MS2-RT level matches", {
  batchesNoMs2 <- getTableByEsql(glue('
    FROM {tableName}
    | WHERE name IS NOT NULL
    | STATS ms2_hits = COUNT() WHERE score_ms2_match IS NOT NULL BY batchname, esi_ion_spec
    | WHERE ms2_hits == 0')
   )
  expect_equal(nrow(batchesNoMs2), 0)
})
