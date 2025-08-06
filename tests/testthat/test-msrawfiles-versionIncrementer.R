
test_that("A new ntsp version can be set in msrawfiles", {
  dbComm <- PythonDbComm()
  currentYear <- format(Sys.Date(), "%y")
  version <- paste0(currentYear, ".9")
  newTableName <- getNewTableName(testIndexName, version)
  if (isTable(dbComm, newTableName))
    deleteTable(dbComm, newTableName)
  
  msrawfilesSetVersion(testIndexName, version)
  
  expect_true(isTable(dbComm, newTableName))
  recs <- getTableAsRecords(dbComm, newTableName)
  newAliasName <- recs[[1]][["dbas_alias_name"]]
  expect_equal(newAliasName, glue("ntsp{currentYear}.9_dbas_unit_tests"))
  deleteTable(dbComm, newTableName)
})

test_that("A badly formated version produces an error", {
  currentYear <- format(Sys.Date(), "%y")
  expect_error(msrawfilesSetVersion(testIndexName, glue("{currentYear}.99")))
  expect_error(msrawfilesSetVersion(testIndexName, "9.9"))
  expect_error(msrawfilesSetVersion(testIndexName, glue("{currentYear}-9")))
  expect_error(msrawfilesSetVersion(testIndexName, glue("{currentYear}.O")))
  expect_error(msrawfilesSetVersion(testIndexName, currentYear))
})

test_that("new Alias name can be generated", {
  oldAliases <- c("ntsp25.1_dbas_foo", "ntsp25.2_dbas_bar")
  version <- "99.9"
  newAliases <- getNewTableName(oldAliases, version)
  expect_equal(newAliases, c("ntsp99.9_dbas_foo", "ntsp99.9_dbas_bar"))
})

test_that("You can get all aliases from a table", {
  dbComm <- PythonDbComm()
  aliases <- getUniqueValues(dbComm, testIndexName, "dbas_alias_name")
  expect_equal(aliases, "ntsp25.2_dbas_unit_tests")
  aliases <- getUniqueValues(dbComm, "ntsp25.2_msrawfiles", "dbas_alias_name")
  expect_gte(length(aliases), 9)
})

test_that("You can change the version of all dbas_alias_names", {
  dbComm <- PythonDbComm()
  oldName <- getUniqueValues(dbComm, testIndexName, "dbas_alias_name")
  #changeAllDbasAliasNames(dbComm, testIndexName, ntspVersion)
  changeAllDbasAliasNames(testIndexName, "99.9")
  q <- dbComm@dsl$Search(using=dbComm@client, index = testIndexName)$source("dbas_alias_name")
  expect_equal(q$execute()$hits$to_list()[[1]]$dbas_alias_name, "ntsp99.9_dbas_unit_tests")
  changeAllDbasAliasNames(testIndexName, ntspVersion)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
