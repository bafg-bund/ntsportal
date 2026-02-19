
test_that("Records for selected batches are returned (DBAS)", {
  records <- getMsrawfilesTestRecords("dbas")
  dirs <- unique(dirname(map_chr(records, \(x) x$path)))
  selectedBatch <- getSelectedMsrawfileBatches(testIndexName, dirs[3], "dbas")
  expect_equal(dirname(selectedBatch[[1]][[1]]$path), dirs[3])
  expect_s3_class(selectedBatch[[1]], "dbasMsrawfilesBatch")
  expect_s3_class(selectedBatch[[1]][[1]], c("dbasMsrawfilesRecord", "msrawfilesRecord", "ntspRecord"))
  selectedBatches <- getSelectedMsrawfileBatches(testIndexName, dirs[2:3], "dbas")
  expect_length(selectedBatches, 2)
})

test_that("Records for selected batches are returned (NTS)", {
  records <- getMsrawfilesTestRecords("nts")
  dirs <- unique(dirname(map_chr(records, \(x) x$path)))
  selectedRecords <- getSelectedMsrawfileBatches(testIndexName, dirs[3], "nts")
  expect_s3_class(selectedRecords[[1]], "ntsMsrawfilesBatch")
  expect_s3_class(selectedRecords[[1]][[1]], c("ntsMsrawfilesRecord", "msrawfilesRecord", "ntspRecord"))
})

test_that("No records are returned for missing directory", {
  records <- getMsrawfilesTestRecords("dbas")
  dirs <- unique(dirname(map_chr(records, \(x) x$path)))
  expect_error(suppressWarnings(getSelectedMsrawfileBatches(testIndexName, "foo", "dbas")))
  expect_warning(selectedRecords <- getSelectedMsrawfileBatches(testIndexName, c(dirs[3], "foo"), "dbas"))
  expect_equal(dirname(selectedRecords[[1]][[1]]$path), dirs[3])
  expect_warning(selectedRecords <- getSelectedMsrawfileBatches(testIndexName, c(dirs[3], test_path("fixtures")), "dbas"))
})


test_that("One can provide a root directory to select batches", {
  batchesTest <- getAllBatchesInDir(testIndexName, rootDirectoryForTestMsrawfiles)
  expect_length(batchesTest, 4)
})

test_that("One can provide multiple directories", {
  dirs <- c(
    file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6"),
    file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
  )
  batchNames <- getAllBatchesInDir(testIndexName, dirs)
  expect_length(batchNames, 2)
})

