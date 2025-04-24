

test_that("A measurement file can be moved and name can be changed", {
  saveDir <- withr::local_tempdir()
  dbComm <- PythonDbComm()
  originalPath <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  newPath <- file.path(saveDir, basename(originalPath))
  expect_true(file.copy(originalPath, newPath))
  idAdded <- makeTestRecord(originalPath, testIndexName, saveDir)
  
  # move test file to temporary location
  expect_true(changeMsrawfilePath(testIndexName, originalPath, newPath, "filesize"))
  refreshTable(dbComm, testIndexName)
  # test the change
  n <- esSearchPaged(testIndexName, searchBody = list(query = list(term = list(path = newPath))), totalSize = 0, sort = "start")$hits$total$value
  expect_equal(n, 1)
  
  # change filename to "blah.mzXML"
  newNewPath <- file.path(dirname(newPath), "blah.mzXML")
  expect_true(suppressMessages(changeMsrawfileFilename(testIndexName, newPath, newNewPath)))
  refreshTable(dbComm, testIndexName)
  # test the change
  n <- esSearchPaged(testIndexName, searchBody = list(query = list(term = list(path = newNewPath))), totalSize = 0, sort = "start")$hits$total$value
  expect_equal(n, 1)
  
  elastic::docs_delete(escon, testIndexName, idAdded)
  refreshTable(dbComm, testIndexName)
  file.remove(list.files(saveDir, f = T))
})


