
test_that("You can connect to elasticsearch with the python client", {
  dbComm <- getDbComm()
  expect_no_error(show(dbComm))
  expect_true(ping(dbComm))
})

test_that("You can copy a table", {
  dbComm <- getDbComm()
  dummyIndex <- "ntsp_foobar"
  if (isTable(dbComm, dummyIndex))
    deleteTable(dbComm, dummyIndex)
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

test_that("You can get unique values of a runtime field", {
  dbComm <- getDbComm()
  batches <- getUniqueValues(dbComm, testIndexName, "batchname")
  expect_length(batches, 4)
})

test_that("You can change the value of a field", {
  dbComm <- getDbComm()
  expect_no_error(replaceValueInField(dbComm, testIndexName, "river", "rhein", "rhein"))
})

test_that("You can change the value of the dbas_alias_name field", {
  dbComm <- getDbComm()
  field <- "feature_table_alias"
  oldName <- getUniqueValues(dbComm, testIndexName, field)
  newName <- "ntsp99.9_feature_unit_tests"
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

test_that("You can retrieve records sorted by a field or fields", {
  dbComm <- getDbComm()
  records <- getTableAsRecords(dbComm, testIndexName, fields = "start")
  startValsUnsorted <- map_chr(records, \(rec) rec$start)
  records <- getTableAsRecords(dbComm, testIndexName, fields = "start", sortField = "start")
  startValsSorted <- map_chr(records, \(rec) rec$start)
  records <- getTableAsRecords(dbComm, testIndexName, fields = "start", sortField = "-start")
  startValsSortedInverse <- map_chr(records, \(rec) rec$start)
  records <- getTableAsRecords(dbComm, testIndexName, fields = "start", sortField = list(start = list(order = "desc")))
  startValsSortedUsingList <- map_chr(records, \(rec) rec$start)
  records <- getTableAsRecords(dbComm, testIndexName, fields = "start", sortField = list(batchname = list(order = "asc"), start = list(order = "desc")))
  startValsSortedUsingListMultiSort <- map_chr(records, \(rec) rec$start)
  
  expect_setequal(startValsUnsorted, startValsSorted)
  expect_false(identical(startValsUnsorted, startValsSorted))
  expect_false(identical(startValsSorted, startValsSortedInverse))
  expect_equal(startValsSortedUsingList, startValsSortedInverse)
  expect_false(identical(startValsSortedUsingList, startValsSortedUsingListMultiSort))
})

test_that("You can append records to a table (ingest)", {
  dbComm <- getDbComm()
  tableName <- "ntsp_temp"
  deleteTable(dbComm, tableName)  # in case it already exists
  createNewTable(dbComm, tableName, "feature")
  emptyRecordList <- getEmptyFeatureRecord()
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
  createNewTable(dbComm, tableName, "feature")
  prds <- list.files(test_path("fixtures", "featureRecordExampleRds"), f = T)
  recs <- readRDS(prds)
  recs <- lapply(recs, unclass)
  startTime <- lubridate::now()
  appendRecords(dbComm, tableName, recs)
  endTime <- lubridate::now()
  expect_lt(endTime - startTime, 0.2)
  deleteTable(dbComm, tableName)
})

test_that("A table can be closed", {
  dbComm <- getDbComm()
  tableName <- "ntsp_temp"
  deleteTable(dbComm, tableName)  # in case it already exists
  createNewTable(dbComm, tableName, "feature")
  emptyRecordList <- getEmptyFeatureRecord()
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
  tb <- getTableByQuery(tableName)
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
  tb <- getTableByQuery(testIndexName, searchBlock = q, fields = "dbas_fp")
  originalFp <- pluck(tb, "dbas_fp", 1)

  addValueToArray(dbComm, testIndexName, searchBlock = q, field = "dbas_fp", value = "BarBarBar")
  tb2 <- getTableByQuery(testIndexName, searchBlock = q, fields = "dbas_fp")
  newFp <- pluck(tb2, "dbas_fp", 1)
  expect_equal(setdiff(newFp, originalFp), "BarBarBar")
  
  removeValueFromArray(dbComm, testIndexName, searchBlock = q, field = "dbas_fp", value = "BarBarBar")
  tb3 <- getTableByQuery(testIndexName, searchBlock = q, fields = "dbas_fp")
  cleanedFp <- pluck(tb3, "dbas_fp", 1)
  expect_equal(cleanedFp, originalFp)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
