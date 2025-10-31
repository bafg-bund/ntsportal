

test_that("You can make an msrawfilesRecord", {
  dbComm <- getDbComm()
  records <- getTableAsRecords(dbComm, testIndexName, recordConstructor = newMsrawfilesRecord)
  expect_s3_class(records[[1]], c("msrawfilesRecord", "ntspRecord", "list"))
})
