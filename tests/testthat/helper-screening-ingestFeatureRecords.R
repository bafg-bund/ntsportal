
testIngestedFeatures <- function(indexName) {
  expect_match(indexName, "ntsp\\d{2}.*_feature_v\\d{12}_unit_tests")
  expect_true(isTable(dbComm, indexName))
  expect_gt(getNrow(dbComm, indexName), 0)
  suppressMessages(
    aliasName <- getTableByQuery(indexName, fields = "feature_table_alias") |> slice(1) |> as.character()
  )
  expect_match(aliasName, "ntsp\\d{2}\\.\\d+_feature_unit_tests")
  suppressMessages(
    startDate <- getTableByQuery(indexName, fields = "start") |> slice(1) |> as.character()
  )
  expect_match(startDate, "\\d{4}-\\d{2}-\\d{2}")
  suppressMessages(
    instName <- getTableByQuery(indexName, fields = "instrument_name") |> slice(1) |> as.character()
  )
  expect_equal(instName, "sediment_tof")
}
