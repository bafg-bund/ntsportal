

test_that("You can compute the linear regression for one compound", {
  tableName <- "ntsp25.3_feature_upb"
  esiSpecSita <- "Sitagliptin_pos_[M+H]+_monoisotopic"
  records <- computeLinearRegressionOneEsiSpec(esiSpecSita, tableName)
  tb <- convertRecordsToTibble(records)
  expect_gte(nrow(tb), 2)
  expect_equal(tb[1, "name", drop = T], "Sitagliptin")
  #updateLinearRegressionTable("ntsp25.3_feature_upb")
})

test_that("A non-existant compound returns an empty list", {
  tableName <- "ntsp25.3_feature_upb"
  records <- computeLinearRegressionOneEsiSpec("Foobar_pos_[M+H]+_monoisotopic", tableName)
  expect_length(records, 0)
})

test_that("You can ingest an analysis_dbas record into a table", {
  tableName <- "ntsp25.3_feature_upb"
  esiSpecSita <- "Sitagliptin_pos_[M+H]+_monoisotopic"
  records <- computeLinearRegressionOneEsiSpec(esiSpecSita, tableName)
  destTableName <- "ntsp25.3_analysis_dbas_test"
  createNewTable(dbComm, destTableName, "analysis_dbas")
  expect_no_error(appendRecords(dbComm, destTableName, records))
  deleteTable(dbComm, destTableName)
})

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal