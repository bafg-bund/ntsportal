

test_that("A dummy record can be added to msrawfiles", {
  dbComm <- getDbComm()
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  templateIdFromMsrawfilesIndex <- "VNSTWpABQ5NoSyLHKzdl"
  saveDir <- withr::local_tempdir()
  suppressMessages(
    idAdded <- addRawfiles(
      rfIndex = testIndexName, 
      templateId = templateIdFromMsrawfilesIndex,
      newPaths = testFile, 
      dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
      prompt = F,
      saveDirectory = saveDir
    )  
  )
  
  importedRecord <- jsonlite::read_json(list.files(saveDir, f = T))
  expect_length(importedRecord[[1]], 84)
  recs <- getTableAsRecords(dbComm, testIndexName, list(query = list(
    regexp = list(path = ".*RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  )))
  pathResult <- recs[[1]]$path

  expect_equal(normalizePath(testFile), pathResult)
  deleteRow(dbComm, testIndexName, list(query = list(ids = list(values = idAdded))))
  file.remove(list.files(saveDir, f = T))
  refreshTable(dbComm, testIndexName)
  numFound <- getNrow(dbComm, testIndexName, list(query = list(
    regexp = list(path = ".*msrawfiles-addRecord/RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  )))
  expect_equal(numFound, 0)
})

test_that("Adding a file with malformed date results in an error", {
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_2206033_no_peaks_test_addRecord_malformedDate.mzXML")
  templateIdFromMsrawfilesIndex <- "VNSTWpABQ5NoSyLHKzdl"
  saveDir <- withr::local_tempdir()
  expect_error(
    suppressMessages(
      suppressWarnings(
        idAdded <- addRawfiles(
          rfIndex = testIndexName, 
          templateId = templateIdFromMsrawfilesIndex,
          newPaths = testFile, 
          dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
          prompt = F,
          saveDirectory = saveDir
        )
      )
    )
  )
  
  dbComm <- getDbComm()
  numFound <- getNrow(dbComm, testIndexName, list(query = list(
    regexp = list(path = ".*RH_pos_2206033_no_peaks_test_addRecord_malformedDate.mzXML")
  )))
  expect_equal(numFound, 0)
})

