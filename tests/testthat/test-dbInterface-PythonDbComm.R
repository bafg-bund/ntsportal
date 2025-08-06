
test_that("You can connect to elasticsearch with the python client", {
  dbComm <- getDbComm()
  expect_no_error(show(dbComm))
  expect_true(ping(dbComm))
})

test_that("You can copy a table", {
  dbComm <- getDbComm()
  dummyIndex <- "ntsp_foobar"
  
  copyTable(dbComm, testIndexName, dummyIndex, "msrawfiles")
  n <- dbComm@dsl$Search(using=dbComm@client, index = dummyIndex)$count()
  
  expect_gte(n, 20)
  expect_true(isTable(dbComm, dummyIndex))
  deleteTable(dbComm, dummyIndex)
})


test_that("You can get all the unique values in a field", {
  dbComm <- getDbComm()
  paths <- getUniqueValues(dbComm, testIndexName, "path")
  expect_gte(length(paths), 20)
  rivers <- getUniqueValues(dbComm, testIndexName, "river")
  expect_contains(rivers, "rhein")
})

test_that("You can change the value of a field", {
  dbComm <- getDbComm()
  expect_no_error(replaceValueInField(dbComm, testIndexName, "river", "rhein", "rhein"))
})

test_that("You can change the value of the dbas_alias_name field", {
  dbComm <- getDbComm()
  field <- "dbas_alias_name"
  oldName <- getUniqueValues(dbComm, testIndexName, field)
  newName <- "ntsp99.9_dbas_unit_tests"
  #newName <- glue("ntsp{ntspVersion}_dbas_unit_tests")  # in case you need to reset
  replaceValueInField(dbComm, testIndexName, field, oldName, newName)
  n <- getNrow(dbComm, testIndexName, list(query = list(term = rlang::list2(!!field := newName))))
  expect_gte(n, 20)
  replaceValueInField(dbComm, testIndexName, field, newName, oldName)
  n2 <- getNrow(dbComm, testIndexName, list(query = list(term = rlang::list2(!!field := oldName))))
  expect_gte(n2, 20)
})

test_that("You can retrieve an entire index as records", {
  dbComm <- getDbComm()
  records <- getTableAsRecords(dbComm, testIndexName)
  expect_s3_class(records[[1]], "ntspRecord")
  expect_gte(length(records), 20)
})

test_that("You can retrieve an index of two fields as a tibble", {
  dbComm <- getDbComm()
  testTibble <- getTableAsTibble(dbComm, testIndexName, fields = c("path", "start"))
  expect_s3_class(testTibble, "tbl_df")
  expect_gte(nrow(testTibble), 20)
  expect_length(testTibble, 2)
})

test_that("Records with different length elements are still coerced into a tibble", {
  minimalRecords <- list(
    list(comp_group = c("A", "B")),
    list(comp_group = "A")
  )
  newTib <- convertRecordsToTibble(minimalRecords)
  expectedTib <- tibble(comp_group = c(list(c("A", "B")), list("A")))
  expect_identical(newTib, expectedTib)
})


test_that("You can append records to a table (ingest)", {
  dbComm <- getDbComm()
  tableName <- "ntsp_temp"
  deleteTable(dbComm, tableName)  # in case it already exists
  createNewTable(dbComm, tableName, "dbas")
  emptyRecordList <- getEmptyRecord()
  appendRecords(dbComm, tableName, emptyRecordList)
  
  resp <- getTableAsRecords(dbComm, tableName)
  expect_contains(names(resp[[1]]), "path")
  checkForAlias(resp[[1]])
  
  deleteTable(dbComm, tableName)
})

test_that("You can ingest a large number of documents quickly", {
  dbComm <- getDbComm()
  tableName <- "ntsp_temp"
  deleteTable(dbComm, tableName)  # in case it already exists
  createNewTable(dbComm, tableName, "dbas")
  prds <- list.files(test_path("fixtures", "featureRecordExampleRds"), f = T)
  recs <- readRDS(prds)
  recs <- lapply(recs, unclass)
  startTime <- lubridate::now()
  appendRecords(dbComm, tableName, recs)
  endTime <- lubridate::now()
  expect_lt(endTime - startTime, 0.1)
  deleteTable(dbComm, tableName)
})

test_that("A table can be closed", {
  dbComm <- getDbComm()
  tableName <- "ntsp_temp"
  deleteTable(dbComm, tableName)  # in case it already exists
  createNewTable(dbComm, tableName, "dbas")
  emptyRecordList <- getEmptyRecord()
  appendRecords(dbComm, tableName, emptyRecordList)
  recs <- getTableAsRecords(dbComm, tableName)
  expect_length(recs, 1)
  resp <- closeTable(dbComm, tableName)
  expect_true(resp)
  expect_error(getTableAsRecords(dbComm, tableName))
  deleteTable(dbComm, tableName) 
})

test_that("A field in a subset of docs can be modified", {
  dbComm <- getDbComm()
  tableName <- "ntsp_temp"
  deleteTable(dbComm, tableName)  # in case it already exists
  copyTable(dbComm, testIndexName, tableName, "msrawfiles")
  qDslSearchBlock <- list(query = list(regexp = list(path = ".*/no-peaks/.*")))
  setValueInField(dbComm, tableName, "dbas_minimum_detections", 0, qDslSearchBlock)
  tb <- getTableAsTibble(dbComm, tableName)
  tbNoPeaks <- filter(tb, grepl("no-peaks", path)) 
  expect_equal(pluck(tb, "dbas_minimum_detections", 1), 2)
  expect_equal(pluck(tbNoPeaks, "dbas_minimum_detections", 1), 0)
  deleteTable(dbComm, tableName) 
})

test_that("Adding a value to a non-array field causes and error", {
  dbComm <- getDbComm()
  expect_error(addValueToArray(dbComm, testIndexName, "licence", "foo"))
})

test_that("Adding an already existing value to an array results in no change", {
  dbComm <- getDbComm()
  recs <- getTableAsRecords(dbComm, testIndexName, fields = "dbas_fp")
  addValueToArray(dbComm, testIndexName, "dbas_fp", "Olmesartan-d6")
  recs2 <- getTableAsRecords(dbComm, testIndexName, fields = "dbas_fp")
  expect_equal(recs, recs2)
})

test_that("Removing a non-existing value from an array results in no change", {
  dbComm <- getDbComm()
  recs <- getTableAsRecords(dbComm, testIndexName, fields = "dbas_fp")
  removeValueFromArray(dbComm, testIndexName, "dbas_fp", "foo")
  recs2 <- getTableAsRecords(dbComm, testIndexName, fields = "dbas_fp")
  expect_equal(recs, recs2)
})

test_that("You can add a value to an array", {
  dbComm <- getDbComm()
  q <- list(query = list(regexp = list(path = ".*KO_06_1.*")))
  tb <- getTableAsTibble(dbComm, testIndexName, searchBlock = q, fields = "dbas_fp")
  originalFp <- pluck(tb, "dbas_fp", 1)

  addValueToArray(dbComm, testIndexName, searchBlock = q, field = "dbas_fp", value = "BarBarBar")
  tb2 <- getTableAsTibble(dbComm, testIndexName, searchBlock = q, fields = "dbas_fp")
  newFp <- pluck(tb2, "dbas_fp", 1)
  expect_equal(setdiff(newFp, originalFp), "BarBarBar")
  
  removeValueFromArray(dbComm, testIndexName, searchBlock = q, field = "dbas_fp", value = "BarBarBar")
  tb3 <- getTableAsTibble(dbComm, testIndexName, searchBlock = q, fields = "dbas_fp")
  cleanedFp <- pluck(tb3, "dbas_fp", 1)
  expect_equal(cleanedFp, originalFp)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
