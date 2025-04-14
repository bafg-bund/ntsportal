


test_that("Dba-screening for a small batch produces a json with peaks", {
  tempSaveDir <- withr::local_tempdir()
  batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
  startTime <- Sys.time()
  pathJson <- dbaScreeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir)
  endTime <- Sys.time()
  
  message("Time needed to processes samples: ", round(difftime(endTime, startTime, units = "secs")), " s")
  
  expect_length(list.files(tempSaveDir), 1)
  jsonLines <- readLines(pathJson)[1:40]
  checkForStation(jsonLines)
  checkForAlias(jsonLines)
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Dba-screening for two batches runs in parallel", {
  tempSaveDir <- withr::local_tempdir()
  batchDirectory <- c(
    file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol"),
    file.path(rootDirectoryForTestMsrawfiles, "no-peaks")
  )
  startTime <- Sys.time()
  pathJson <- dbaScreeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir, numParallel = 2)
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
  
  dbaScreeningSelectedBatchesSlurm(
    testIndexName, 
    rootDirectoryForTestMsrawfiles, 
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

