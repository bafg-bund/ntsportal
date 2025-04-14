

test_that("A measurement file can be moved and name can be changed", {
  saveDir <- withr::local_tempdir()
  originalPath <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  newPath <- file.path(saveDir, basename(originalPath))
  expect_true(file.copy(originalPath, newPath))
  idAdded <- makeTestRecord(originalPath, testIndexName, saveDir)
  
  # move test file to temporary location
  expect_true(changeMsrawfilePath(testIndexName, originalPath, newPath, "filesize"))
  
  # test the change
  Sys.sleep(1)
  expect_true(pathInIndex(newPath, testIndexName))
  
  # change filename to "blah.mzXML"
  newNewPath <- file.path(dirname(newPath), "blah.mzXML")
  expect_true(suppressMessages(changeMsrawfileFilename(testIndexName, newPath, newNewPath)))
  
  # test the change
  Sys.sleep(1)
  expect_true(pathInIndex(newNewPath, testIndexName))
  
  elastic::docs_delete(escon, testIndexName, idAdded)
  file.remove(list.files(saveDir, f = T))
})


