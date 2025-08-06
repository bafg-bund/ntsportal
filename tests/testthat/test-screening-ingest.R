
test_that("One file is uploaded in Elasticsearch", {
  dbComm <- getDbComm()
  esIndices <- ingest(test_path("fixtures", "featureRecordExampleRds"), ingestPipeline = "ingest-feature-unit-tests")
  indexName <- esIndices[[1]]$ntsp
  expect_match(indexName, "ntsp\\d{2}.*_dbas_v\\d{12}_unit_tests")
  expect_true(isTable(dbComm, indexName))
  expect_gt(getNrow(dbComm, indexName), 0)
  aliasName <- getTableAsTibble(dbComm, indexName, fields = "dbas_alias_name") |> slice(1) |> as.character()
  expect_match(aliasName, "ntsp\\d{2}\\.\\d+_dbas_unit_tests")
  startDate <- getTableAsTibble(dbComm, indexName, fields = "start") |> slice(1) |> as.character()
  expect_match(startDate, "\\d{4}-\\d{2}-\\d{2}")
  deleteTable(dbComm, indexName)
})

test_that("You can read an RDS file to a list of records", {
  rdsPath <- getRdsFilePaths(test_path("fixtures", "featureRecordExampleRds"))
  expect_length(rdsPath, 1)
  recs <- readRdsToRecords(rdsPath)
  expect_s3_class(recs, c("Records", "R6"))
  expect_type(recs$recs[[1]], "list")
})

test_that("You can add the current time to a list of records", {
  rdsPath <- getRdsFilePaths(test_path("fixtures", "featureRecordExampleRds"))
  recs <- readRdsToRecords(rdsPath)
  recs$addImportTime()
  expect_match(recs$recs[[1]]$date_import, "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$")
})

test_that("You can add the RDS path to a list of records", {
  jsonPath <- getRdsFilePaths(test_path("fixtures", "featureRecordExampleRds"))
  recs <- readRdsToRecords(jsonPath)
  expect_match(recs$rdsPath, "ntsportal-featureRecord.*-part-a\\.RDS$")
})

test_that("You can create multiple indices", {
  aliasNames <- c("ntsp99.9_dbas_foo", "ntsp99.9_dbas_bar")
  timestamp <- "00000000000000"
  dbComm <- getDbComm()
  indexMappingPath <- fs::path_package("ntsportal", "mappings")
  pairs <- pyIngestModule$createIndexAddAlias(aliasNames, dbComm@client, timestamp, indexMappingPath)
  expect_true(all(map_lgl(pairs, function(pair) isTable(dbComm, pair[[1]]))))
  expect_true(all(map_lgl(names(pairs), function(aliasName) isTable(dbComm, aliasName))))
  walk(pairs, function(pair) deleteTable(dbComm, pair[[1]]))
})

test_that("An error in python results in an error message", {
  jsonPath <- getRdsFilePaths(test_path("fixtures", "featureRecordExampleRds"))
  dbComm <- getDbComm()
  indexTimeStamp <- format(lubridate::now(), "%y%m%d%H%M%S")
  indexMappingPath <- fs::path_package("ntsportal", "mappings")
  recs <- readRdsToRecords(jsonPath)
  tfile <- withr::local_tempfile()
  log_appender(appender_file(tfile))
  recs$changeDbasAliasName("TESTFAIL")
  
  executePyIngestModule(recs, dbComm, indexTimeStamp, indexMappingPath)
  errorLines <- readLines(tfile)
  expect_match(errorLines[1], "ntsportal-featureRecord.*-part-a")
  file.remove(tfile)
  log_appender(appender_console)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
