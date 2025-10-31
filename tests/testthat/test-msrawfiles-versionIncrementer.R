
test_that("A new ntsp version can be set in msrawfiles", {
  dbComm <- getDbComm()
  currentYear <- format(Sys.Date(), "%y")
  version <- paste0(currentYear, ".9")
  newTableName <- getNewTableName(testIndexName, version)
  if (isTable(dbComm, newTableName))
    deleteTable(dbComm, newTableName)
  
  msrawfilesSetVersion(testIndexName, version)
  
  expect_true(isTable(dbComm, newTableName))
  recs <- getTableAsRecords(dbComm, newTableName)
  newAliasName <- recs[[1]][["feature_table_alias"]]
  expect_equal(newAliasName, glue("ntsp{currentYear}.9_feature_unit_tests"))
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
  oldAliases <- c("ntsp25.1_feature_foo", "ntsp25.2_feature_bar")
  version <- "99.9"
  newAliases <- getNewTableName(oldAliases, version)
  expect_equal(newAliases, c("ntsp99.9_feature_foo", "ntsp99.9_feature_bar"))
})

test_that("You can get all aliases from a table", {
  dbComm <- getDbComm()
  aliases <- getUniqueValues(dbComm, testIndexName, "feature_table_alias")
  expect_equal(aliases, glue("ntsp{ntspVersion}_feature_unit_tests"))
})

test_that("You can change the version of all dbas_alias_names", {
  dbComm <- getDbComm()
  oldName <- getUniqueValues(dbComm, testIndexName, "feature_table_alias")
  #changeAllDbasAliasNames(dbComm, testIndexName, ntspVersion)
  changeAllFeatureAliasNames(testIndexName, "99.9")
  q <- dbComm@dsl$Search(using=dbComm@client, index = testIndexName)$source("feature_table_alias")
  expect_equal(q$execute()$hits$to_list()[[1]]$feature_table_alias, "ntsp99.9_feature_unit_tests")
  changeAllFeatureAliasNames(testIndexName, ntspVersion)
  q <- dbComm@dsl$Search(using=dbComm@client, index = testIndexName)$source("feature_table_alias")
  expect_equal(q$execute()$hits$to_list()[[1]]$feature_table_alias, glue("ntsp{ntspVersion}_feature_unit_tests"))
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
