

test_that("You can make an msrawfilesRecord", {
  dbComm <- getDbComm()
  query <- list(query = list(regexp = list(path = ".*no-peaks.*")))
  records <- getMsrawfilesTestRecords("no_class")
  recordsWithClass <- lapply(records, newMsrawfilesRecord)
  expect_s3_class(recordsWithClass[[1]], c("msrawfilesRecord", "ntspRecord", "list"))
  recordsWithClass <- lapply(records, newDbasMsrawfilesRecord)
  expect_s3_class(recordsWithClass[[1]], c("dbasMsrawfilesRecord", "msrawfilesRecord", "ntspRecord", "list"))
  recordsWithClass <- lapply(records, newNtsMsrawfilesRecord)
  expect_s3_class(recordsWithClass[[1]], c("ntsMsrawfilesRecord", "msrawfilesRecord", "ntspRecord", "list"))
})
