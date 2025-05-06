

test_that("You can get the spectral library path", {
  pth <- getSpectralLibraryPath(testIndexName)
  expect_match(pth, "/CSL_v\\d\\d\\.\\d\\.db$")
})

test_that("A small CSL can be converted to a list of records", {
  records <- getExampleCslAsRecords()
 
  expect_s3_class(records, "SpecLibRecords")
  expect_gte(length(records$records[[1]]$ms2), 2)
  expect_gte(length(records$records[[1]]$rtt), 4)
  expect_false("experiment_id" %in% names(records$records[[1]]))
  expect_true("comp_group" %in% names(records$records[[1]]))
})

test_that("A small CSL can be sent to elastic", {
  tempTable <- "ntsp99.9_spectral_library_test"
  records <- getExampleCslAsRecords()
  ingestSpectralLibrary(records, tempTable)
  dbComm <- getDbComm()
  tableNrows <- getNrow(dbComm, tempTable)
  expect_gte(tableNrows, 5)  
  deleteTable(dbComm, tempTable)
})
