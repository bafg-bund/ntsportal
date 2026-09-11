
test_that("Records for selected batches are returned (DBAS)", {
  records <- getMsrawfilesTestRecords("dbas")
  dirs <- unique(dirname(map_chr(records, \(x) x$path)))
  selectedBatch <- getSelectedMsrawfileBatches(testIndexName, dirs[3], "dbas")
  expect_length(selectedBatch, 1)
  expect_equal(dirname(selectedBatch[[1]][[1]]$path), dirs[3])
  expect_s3_class(selectedBatch[[1]], "dbasMsrawfilesBatch")
  expect_s3_class(selectedBatch[[1]][[1]], c("dbasMsrawfilesRecord", "msrawfilesRecord", "ntspRecord"))
  selectedBatches <- getSelectedMsrawfileBatches(testIndexName, dirs[grepl("olmesartan-d6-bisoprolol|no-peaks", dirs)], "dbas")
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

test_that("Additional blank records are added to a batch if these are listed", {
  batchName <- "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6-bisoprolol"
  batch <- getSelectedMsrawfileBatches(testIndexName, batchName, "dbas")
  paths <- getField(batch[[1]], "path")
  expect_length(batch[[1]], 11)
  expect_contains(basename(paths), "BL_1_pos.mzXML")
})

test_that("One additional blank record can be added to a batch if listed", {
  batchName <- "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6"
  batch <- getSelectedMsrawfileBatches(testIndexName, batchName, "dbas")
  paths <- getField(batch[[1]], "path")
  expect_length(batch[[1]], 5)
  expect_contains(basename(paths), "RH_pos_20220602_no_peaks_MQ_test.mzXML")
})

test_that("A batch with only blanks is removed", {
  r <- getMsrawfilesTestRecords("dbas")
  recs1 <- r[dirname(getField(r, "path")) == "/beegfs/nts/ntsportal/msrawfiles/unit_tests" & getField(r, "blank")] 
  recs2 <- r[dirname(getField(r, "path")) == "/beegfs/nts/ntsportal/msrawfiles/unit_tests/olmesartan-d6"]
  testRecs <- c(recs1, recs2)
  suppressMessages(batches <- recordsToBatches(testRecs, testIndexName))
  expect_length(batches, 1)
  expect_match(names(batches), "olmesartan-d6$")
})
