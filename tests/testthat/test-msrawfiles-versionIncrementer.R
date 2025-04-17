
test_that("A new ntsp version can be set in msrawfiles", {
  dbComm <- newPythonComm()
  version <- "99.9"
  
  msrawfilesSetVersion(dbComm, testIndexName, version)
  newTableName <- getNewTableName(testIndexName, version)
  expect_true(isTable(dbComm, newTableName))
  newAliasName <- esSearchPaged(
    newTableName, 
    totalSize = 1, 
    sort = "start", 
    source = "dbas_alias_name"
  )$hits$hits[[1]][["_source"]]$dbas_alias_name
  expect_equal(newAliasName, "ntsp99.9_dbas_unit_tests")
  deleteTable(dbComm, newTableName)
})

test_that("A badly formated version produces an error", {
  dbComm <- newPythonComm()
  expect_error(msrawfilesSetVersion(dbComm, testIndexName, "99.99"))
  expect_error(msrawfilesSetVersion(dbComm, testIndexName, "9.9"))
  expect_error(msrawfilesSetVersion(dbComm, testIndexName, "99-9"))
  expect_error(msrawfilesSetVersion(dbComm, testIndexName, "99.O"))
  expect_error(msrawfilesSetVersion(dbComm, testIndexName, "99"))
})

test_that("new Alias name can be generated", {
  oldAliases <- c("ntsp_dbas_foo", "ntsp25.1_dbas_bar")
  version <- "99.9"
  newAliases <- getNewTableName(oldAliases, version)
  expect_equal(newAliases, c("ntsp99.9_dbas_foo", "ntsp99.9_dbas_bar"))
})

test_that("You can get all aliases from a table", {
  dbComm <- newPythonComm()
  aliases <- getAllDbasAliasNames(dbComm, testIndexName)
  expect_equal(aliases, "ntsp25.1_dbas_unit_tests")
  aliases <- getAllDbasAliasNames(dbComm, "ntsp_msrawfiles")
  expect_gte(length(aliases), 10)
})

test_that("You can change the value of the dbas_alias_name field", {
  dsl <- import("elasticsearch.dsl")
  dbComm <- newPythonComm()
  oldName <- getAllDbasAliasNames(dbComm, testIndexName)
  newName <- "ntsp99.9_dbas_unit_tests"
  #newName <- "ntsp25.1_dbas_unit_tests"
  changeDbasAliasName(dbComm, testIndexName, oldName, newName)
  n <- dsl$Search(using=dbComm, index = testIndexName)$query("term", dbas_alias_name = newName)$count()
  expect_gte(n, 20)
  changeDbasAliasName(dbComm, testIndexName, newName, oldName)
})

test_that("You can change the version of all dbas_alias_names", {
  dsl <- import("elasticsearch.dsl")
  dbComm <- newPythonComm()
  oldName <- getAllDbasAliasNames(dbComm, testIndexName)
  changeAllDbasAliasNames(dbComm, testIndexName, "99.9")
  q <- dsl$Search(using=dbComm, index = testIndexName)$source("dbas_alias_name")
  expect_equal(q$execute()$hits$to_list()[[1]]$dbas_alias_name, "ntsp99.9_dbas_unit_tests")
  changeAllDbasAliasNames(dbComm, testIndexName, ntspVersion)
})

test_that("You can copy a table", {
  dsl <- import("elasticsearch.dsl")
  dbComm <- newPythonComm()
  
  dummyIndex <- "ntsp_foobar"
  copyTable(dbComm, testIndexName, dummyIndex, "msrawfiles")
  n <- dsl$Search(using=dbComm, index = dummyIndex)$count()
  expect_gte(n, 20)
  expect_true(isTable(dbComm, dummyIndex))
  deleteTable(dbComm, dummyIndex)
})
