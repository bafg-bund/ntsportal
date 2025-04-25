


test_that("Records for selected batches are returned", {
  records <- getAllMsrawfilesRecords(testIndexName)
  dirs <- extractDirs(records)
  selectedRecords <- getSelectedMsrawfileBatches(testIndexName, dirs[3])
  expect_equal(dirname(selectedRecords[[1]][[1]]$path), dirs[3])
})

test_that("Unprocessed files can be collected from msrawfiles", {
  prepareExampleFeatureIndex(escon, ntspVersion)
  unprocessedBatches <- getUnprocessedMsrawfileBatches(testIndexName, screeningType = "dbasTest")
  expect_false(all(grepl("no_peaks", names(unprocessedBatches))))
  expect_true(any(grepl("olmesartan-d6-bisoprolol", names(unprocessedBatches))))
  expect_s3_class(unprocessedBatches[[1]][[1]], "msrawfilesRecord")
  expect_equal(length(unprocessedBatches), 3)
  
  removeExampleFeatureIndex(escon, ntspVersion)
})

test_that("File directories can be optained from test index", {
  dirnames <- getDirsInFeatureIndex(testIndexName)
  expect_length(dirnames, 4)
})

test_that("splitRecordsByDir works with length 0 input", {
  records <- splitRecordsByDir(list())
  expect_length(records, 0)
})

test_that("Unprocessed directories are returned", {
  records <- getAllMsrawfilesRecords(testIndexName)
  records <- getUnprocessedRecords(records, "dbasTest", ntspVersion = ntspVersion)
  expect_length(records, 20)
  expect_s3_class(records[[1]], "msrawfilesRecord")
})

test_that("One can provide a root directory to select batches", {
  allRecords <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  allRecordsAgain <- getSelectedRecords(allRecords, rootDirectoryForTestMsrawfiles)
  expect_equal(allRecords, allRecordsAgain)
})

test_that("One can provide multiple directories", {
  allRecords <- readRDS(test_path("fixtures", "msrawfilesTestRecords", "allRecords.RDS"))
  dirs <- c(
    file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6"),
    file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
  )
  
  filteredRecords <- getSelectedRecords(allRecords, dirs)
  expect_lt(length(filteredRecords), length(allRecords))
})

