

test_that("A backup can be created with the current date and cannot be overwritten unless forced", {
  newName <- createBackupMsrawfiles(testIndexName)
  dbComm <- getDbComm()
  expect_true(isTable(dbComm, newName))
  expect_error(createBackupMsrawfiles(testIndexName))
  newName2 <- createBackupMsrawfiles(testIndexName, overwrite = TRUE)
  deleteTable(dbComm, newName2)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
