
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

test_that("You can retrieve an index as a tibble", {
  dbComm <- getDbComm()
  testTibble <- getTableAsTibble(dbComm, testIndexName)
  expect_s3_class(testTibble, "tbl_df")
  expect_gte(nrow(testTibble), 20)
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
  checkForAlias(resp[[1]]$dbas_alias_name)
  
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
  copyTable(dbComm, testIndexName, tableName, "msrawfiles")
  qDslSearchBlock <- list(query = list(regexp = list(path = ".*/no-peaks/.*")))
  setValueInField(dbComm, tableName, "dbas_minimum_detections", 0, qDslSearchBlock)
  tb <- getTableAsTibble(dbComm, tableName)
  tbNoPeaks <- filter(tb, grepl("no-peaks", path)) 
  expect_equal(pluck(tb, "dbas_minimum_detections", 1), 2)
  expect_equal(pluck(tbNoPeaks, "dbas_minimum_detections", 1), 0)
  deleteTable(dbComm, tableName) 
})


