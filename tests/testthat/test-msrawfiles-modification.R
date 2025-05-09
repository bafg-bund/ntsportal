

test_that("A measurement file can be moved and name can be changed", {
  saveDir <- withr::local_tempdir()
  dbComm <- getDbComm()
  originalPath <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  removeExtraTestFile(basename(originalPath))
  newPath <- file.path(saveDir, basename(originalPath))
  expect_true(file.copy(originalPath, newPath))
  idAdded <- makeTestRecord(originalPath, testIndexName, saveDir)
  
  # move test record path to temporary location
  expect_true(changeMsrawfilePath(testIndexName, originalPath, newPath, "filesize"))
  refreshTable(dbComm, testIndexName)
  # test the change
  n <- getNrow(dbComm, testIndexName, searchBlock = list(query = list(regexp = list(path = ".*test_addRecord.*"))))
  expect_equal(n, 1)
  
  # change filename
  newNewPath <- file.path(dirname(newPath), "blah.mzXML")
  removeExtraTestFile(basename(newNewPath))
  expect_true(suppressMessages(changeMsrawfileFilename(testIndexName, newPath, newNewPath)))
  refreshTable(dbComm, testIndexName)
  
  # test the change
  n <- getNrow(dbComm, testIndexName, searchBlock = list(query = list(regexp = list(path = ".*blah.*"))))
  expect_equal(n, 1)
  
  # cleanup
  deleteRow(dbComm, testIndexName, list(query = list(ids = list(values = idAdded))))
  nNew <- getNrow(dbComm, testIndexName, searchBlock = list(query = list(regexp = list(path = ".*blah.*"))))
  expect_equal(nNew, 0)
  file.remove(list.files(saveDir, f = T))
})


