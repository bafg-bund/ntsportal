
connectNtsportal()


test_that("Dba-screening for a small batch produces a json with peaks", {
  tempSaveDir <- withr::local_tempdir()
  index <- "ntsp_index_msrawfiles_unit_tests"
  batchDirectory <- "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol"
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

test_that("Dba-screening for two batches runs in parallel", {
  tempSaveDir <- withr::local_tempdir()
  index <- "ntsp_index_msrawfiles_unit_tests"
  batchDirectory <- c(
    "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol",
    "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/no-peaks"
  )
  startTime <- Sys.time()
  pathJson <- dbaScreeningSelectedBatches(index, batchDirectory, tempSaveDir, numParallel = 2)
  endTime <- Sys.time()
  
  message("Time needed to processes samples: ", round(difftime(endTime, startTime, units = "secs")), " s")
  
  expect_length(list.files(tempSaveDir), 2)
  jsonLines <- readLines(pathJson[2])[1:40]
  expect_true(any(grepl("station", jsonLines)))
  jsonLines <- readLines(pathJson[1])
  expect_true(any(grepl("path.*no_peaks", jsonLines)))
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Dba-screening array job file and records in batches are produced", {
  tempSaveDir <- withr::local_tempdir()
  index <- "ntsp_index_msrawfiles_unit_tests"
  
  dbaScreeningSelectedBatchesSlurm(
    index, 
    "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/", 
    tempSaveDir,
    "testEmail@test.de"
  )
  
  expect_length(list.files(tempSaveDir),3)
  linesJobFile <- readLines(file.path(tempSaveDir, "arrayDbaScreening.sbatch"))
  expect_equal(linesJobFile[13], "#SBATCH --array=1-4%10")
  expect_match(linesJobFile[6], "testEmail@test.de")
  expect_match(linesJobFile[4], tempSaveDir)
  expect_match(linesJobFile[56], tempSaveDir)
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

