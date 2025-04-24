
test_that("You can connect to elasticsearch with the python client", {
  pyComm <- PythonDbComm()
  expect_no_error(show(pyComm))
  expect_true(ping(pyComm))
})

test_that("You can copy a table", {
  dbComm <- PythonDbComm()
  dummyIndex <- "ntsp_foobar"
  
  copyTable(dbComm, testIndexName, dummyIndex, "msrawfiles")
  n <- dbComm@dsl$Search(using=dbComm@client, index = dummyIndex)$count()
  
  expect_gte(n, 20)
  expect_true(isTable(dbComm, dummyIndex))
  deleteTable(dbComm, dummyIndex)
})
