
test_that("One file is uploaded in Elasticsearch", {
  dbComm <- getDbComm()
  esIndices <- ingest(test_path("fixtures", "featureRecordExampleJson"))
  indexName <- esIndices[[1]]$ntsp
  expect_match(indexName, "ntsp\\d{2}.*_dbas_v\\d{12}_unit_tests")

  expect_true(dbComm@client$indices$exists(index=indexName)$body)
  resp <- dbComm@client$count(index=indexName)
  expect_gt(resp$body$count, 0)
  resp <- dbComm@client$search(index=indexName, size=1, source="dbas_alias_name")
  expect_match(resp$body$hits$hits[[1]][["_source"]]$dbas_alias_name, "ntsp\\d{2}\\.\\d+_dbas_unit_tests")
  deleteTable(dbComm, indexName)
})

test_that("You can read a JSON file to a list of records", {
  jsonPath <- getJsonFilePaths(test_path("fixtures", "featureRecordExampleJson"))
  expect_length(jsonPath, 1)
  recs <- readJsonToRecords(jsonPath)
  expect_s3_class(recs, c("Records", "R6"))
  expect_s3_class(recs$recs[[1]], "ntspRecord")
})


test_that("You can add the current time to a list of records", {
  jsonPath <- getJsonFilePaths(test_path("fixtures", "featureRecordExampleJson"))
  recs <- readJsonToRecords(jsonPath)
  recs$addImportTime()
  expect_match(recs$recs[[1]]$date_import, "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$")
})

test_that("You can add the json path to a list of records", {
  jsonPath <- getJsonFilePaths(test_path("fixtures", "featureRecordExampleJson"))
  recs <- readJsonToRecords(jsonPath)
  expect_match(recs$jsonPath, "olmesartan-d6-bisoprolol-part-a\\.json.gz$")
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

test_that("An error in python results in a error message", {
  jsonPath <- getJsonFilePaths(test_path("fixtures", "featureRecordExampleJson"))
  dbComm <- getDbComm()
  indexTimeStamp <- format(lubridate::now(), "%y%m%d%H%M%S")
  indexMappingPath <- fs::path_package("ntsportal", "mappings")
  recs <- readJsonToRecords(jsonPath)
  tfile <- withr::local_tempfile()
  log_appender(appender_file(tfile))
  recs$changeDbasAliasName("TESTFAIL")
  
  executePyIngestModule(recs, dbComm, indexTimeStamp, indexMappingPath)
  errorLines <- readLines(tfile)
  expect_match(errorLines[1], "-olmesartan-d6-bisoprolol-part-a")
  file.remove(tfile)
  log_appender(appender_console)
})
