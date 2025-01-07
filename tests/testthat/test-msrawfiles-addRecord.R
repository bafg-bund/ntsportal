
connectNtsportal()

test_that("A dummy record can be added to msrawfiles", {
  
  indexName <- "ntsp_index_msrawfiles_unit_tests"
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  templateIdFromMsrawfilesIndex <- "VNSTWpABQ5NoSyLHKzdl"
  saveDir <- withr::local_tempdir()
  suppressMessages(
    idAdded <- addRawfiles(
      rfIndex = indexName, 
      templateId = templateIdFromMsrawfilesIndex,
      newPaths = testFile, 
      dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
      prompt = F,
      saveDirectory = saveDir
    )  
  )
  
  
  importedRecord <- jsonlite::read_json(list.files(saveDir, f = T))
  expect_length(importedRecord[[1]], 88)
  pathResult <- elastic::Search(escon, indexName, source = "path", body = list(query = list(
    regexp = list(path = ".*RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  )))$hits$hits[[1]][["_source"]]$path
  expect_equal(normalizePath(testFile), pathResult)
  
  elastic::docs_delete(escon, indexName, idAdded)
  file.remove(list.files(saveDir, f = T))
  
  Sys.sleep(1)
  numFound <- elastic::Search(escon,
                              indexName,
                              source = "path",
                              body = list(query = list(
                                regexp = list(path = ".*msrawfiles-addRecord/RH_pos_20220603_no_peaks_test_addRecord.mzXML")
                              )))$hits$total$value
  expect_equal(numFound, 0)
})

test_that("Adding a file with malformed date results in an error", {
  indexName <- "ntsp_index_msrawfiles_unit_tests"
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_2206033_no_peaks_test_addRecord_malformedDate.mzXML")
  templateIdFromMsrawfilesIndex <- "VNSTWpABQ5NoSyLHKzdl"
  saveDir <- withr::local_tempdir()
  expect_error(
    suppressMessages(
      suppressWarnings(
        idAdded <- addRawfiles(
          rfIndex = indexName, 
          templateId = templateIdFromMsrawfilesIndex,
          newPaths = testFile, 
          dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
          prompt = F,
          saveDirectory = saveDir
        )
      )
    )
  )
})

