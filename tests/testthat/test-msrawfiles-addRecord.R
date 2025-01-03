
connectNtsportal()

test_that("A dummy record can be added to msrawfiles", {
  saveDir <- withr::local_tempdir()
  indexName <- "ntsp_index_msrawfiles_unit_tests"
  testFile <- test_path("fixtures", "msrawfiles-addRecord", "RH_pos_20220603_no_peaks_test_addRecord.mzXML")
  idAdded <- addRawfiles(
    escon, indexName, "VNSTWpABQ5NoSyLHKzdl",
    newPaths = testFile, rootMeasDir = test_path("fixtures", "msrawfiles-addRecord"),
    prompt = F,
    saveDirectory = saveDir
  )
  
  importedRecord <- jsonlite::read_json(list.files(saveDir, f = T))
  expect_length(importedRecord[[1]], 88)
  pathResult <- elastic::Search(escon, indexName, source = "path", body = list(query = list(regexp = list(path = ".*RH_pos_20220603_no_peaks_test_addRecord.mzXML"))))$hits$hits[[1]][["_source"]]$path
  expect_equal(normalizePath(testFile), pathResult)
  
  idAdded <- stringr::str_remove_all(idAdded, "\"|[:space:]")
  elastic::docs_delete(escon, indexName, idAdded)
  file.remove(list.files(saveDir, f = T))
  
  Sys.sleep(1)
  numFound <- elastic::Search(escon, indexName, source = "path", body = list(query = list(regexp = list(path = ".*msrawfiles-addRecord/RH_pos_20220603_no_peaks_test_addRecord.mzXML"))))$hits$total$value
  expect_equal(numFound, 0)
})
