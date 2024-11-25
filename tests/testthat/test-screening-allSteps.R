
connectNtsportal()


test_that("Dba-screening for a small batch produces a json with peaks", {
  tempSaveDir <- withr::local_tempdir()
  index <- "ntsp_index_msrawfiles_unit_tests"
  batchDirectory <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol/"
  startTime <- Sys.time()
  pathJson <- dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir)
  endTime <- Sys.time()
  
  message("Time needed to processes samples: ", round(difftime(endTime, startTime, units = "secs")), " s")
  
  expect_length(list.files(tempSaveDir), 1)
  jsonLines <- readLines(pathJson)[1:40]
  expect_true(any(grepl("station", jsonLines)))
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

