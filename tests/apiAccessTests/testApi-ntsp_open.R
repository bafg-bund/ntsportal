
# Set new credentials

resetConnectionCredentials()

dbasBfgTable <- "ntsp25.1_dbas_bfg"
wildCardTable <- "ntsp25.1_dbas*"

# Once tests complete, return to old state
resetConnectionCredentials()

test_that("ntsp_open can retrieve records from ntsp??.?_dbas_bfg", {
  dbComm <- getDbComm()
  expect_error(getTableAsRecords(dbComm, testIndexName))
  recs <- getTableAsRecords(
    dbComm, 
    dbasBfgTable, 
    searchBlock = list(query = list(term = list(name = "Praziquantel"))),
    fields = c("name", "mz", "station", "path")
  )
  expect_s3_class(recs[[1]], "ntspRecord")
  expect_gte(length(recs), 1)
})

test_that("The open user can specify a wildcard pattern to retrieve docs from multiple indices", {
  dbComm <- getDbComm()
  recs <- getTableAsRecords(
    dbComm, 
    wildCardTable, 
    searchBlock = list(query = list(term = list(name = "Triclocarban"))),
    fields = "matrix"
  )
  expect_s3_class(recs[[1]], "ntspRecord")
  expect_gte(length(recs), 1)
  expect_setequal(unique(sapply(recs, \(rec) rec$matrix)), c("spm", "water")) 
})


# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
