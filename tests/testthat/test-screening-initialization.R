
connectNtsportal()

test_that("Records for selected batches are returned", {
  index <- "ntsp_index_msrawfiles_unit_tests"
  records <- getAllMsrawfilesRecords(index)
  dirs <- extractDirs(records)
  selectedRecords <- getSelectedMsrawfileBatches(index, dirs[1:2])
  expect_equal(dirname(selectedRecords[[2]][[1]]$path), dirs[2])
})

test_that("Unprocessed files can be collected from msrawfiles", {
  index <- "ntsp_index_msrawfiles_unit_tests"
  unprocessedMsFiles <- getUnprocessedMsrawfileBatches(index, "dbasTest")
  
  expect_s3_class(unprocessedMsFiles[[1]][[1]], "msrawfileRecord")
  expect_equal(length(unprocessedMsFiles), 4)
})

test_that("File directories can be optained from test index", {
  dirnames <- getDirsInFeatureIndex("ntsp_index_msrawfiles_unit_tests")
  expect_length(dirnames, 4)
})

test_that("Unprocessed directories are returned", {
  index <- "ntsp_index_msrawfiles_unit_tests"
  records <- getAllMsrawfilesRecords(index)
  records <- getUnprocessedRecords(records, "msrawfilesTest")
  expect_length(records, 0)
})

test_that("splitRecordsByDir works with length 0 input", {
  records <- splitRecordsByDir(list())
  expect_length(records, 0)
})

test_that("Unprocessed directories are returned", {
  index <- "ntsp_index_msrawfiles_unit_tests"
  records <- getAllMsrawfilesRecords(index)
  records <- getUnprocessedRecords(records, "dbasTest")
  expect_length(records, 19)
  expect_s3_class(records[[1]], "msrawfileRecord")
})

test_that("One can provide a root directory to select batches", {
  allRecords <- entireTestMsrawfilesIndex()
  rootDir <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/"
  allRecordsAgain <- getSelectedRecords(allRecords, rootDir)
  expect_equal(allRecords, allRecordsAgain)
})

test_that("One can provide multiple directories", {
  allRecords <- entireTestMsrawfilesIndex()
  dirs <- c("/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6",
            "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol")
  filteredRecords <- getSelectedRecords(allRecords, dirs)
  expect_lt(length(filteredRecords), length(allRecords))
})
