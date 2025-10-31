
test_that("nt-screening for a small batch produces an RDS with peaks", {
  tempSaveDir <- withr::local_tempdir()
  recs <- getOneSampleRecords()
  startTime <- Sys.time()
  resultPath <- ntScreeningOneBatch(recs, tempSaveDir)
  endTime <- Sys.time()
  message("Time needed to processes files: ", round(difftime(endTime, startTime, units = "secs")), " s")
  expect_length(list.files(tempSaveDir), 1)
  featureRecs <- readRDS(resultPath)
  expect_true(all(map_int(featureRecs, \(x) x$area_internal_standard) == 42956))
})

test_that("Dba-screening for a small batch produces an RDS with peaks", {
  tempSaveDir <- withr::local_tempdir()
  batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
  startTime <- Sys.time()
  pathRds <- screeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir)
  endTime <- Sys.time()
  
  message("Time needed to processes one batch dbas: ", round(difftime(endTime, startTime, units = "secs")), " s")
  
  expect_length(list.files(tempSaveDir), 1)
  recs <- readRDS(pathRds)
  expect_contains(names(recs[[1]]), "path")
  checkForAlias(recs[[1]])
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Dba-screening for two batches runs in parallel", {
  # need to install package before running this test
  tempSaveDir <- withr::local_tempdir()
  batchDirectory <- c(
    file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol"),
    file.path(rootDirectoryForTestMsrawfiles, "no-peaks")
  )
  startTime <- Sys.time()
  pathRds <- screeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir, numParallel = 2)
  endTime <- Sys.time()
  
  message("Time needed to processes samples: ", round(difftime(endTime, startTime, units = "secs")), " s")
  
  expect_length(list.files(tempSaveDir), 2)
  recs <- c(readRDS(pathRds[1]), readRDS(pathRds[2]))
  expect_contains(list_c(map(recs, \(x) names(x))), "path")
  allPaths <- basename(map_chr(recs, \(rec) rec$path))
  expect_true(any(grepl("no_peaks", allPaths)))
  file.remove(list.files(tempSaveDir, full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("DBA-Screening array job file and records in batches are produced", {
  tempSaveDir <- withr::local_tempdir()
  
  screeningSelectedBatchesSlurm(
    msrawfileIndex=testIndexName, 
    batchDirs=file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol"), 
    saveDirectory=tempSaveDir,
    email="testEmail@test.de"
  )
  
  expect_length(list.files(tempSaveDir),3)
  linesJobFile <- readLines(file.path(tempSaveDir, "arrayScreening.sbatch"))
  expect_equal(linesJobFile[13], "#SBATCH --array=1-1%10")
  expect_match(linesJobFile[6], "testEmail@test.de")
  expect_match(linesJobFile[4], tempSaveDir)
  expect_match(linesJobFile[56], tempSaveDir)
  expect_match(linesJobFile[56], "dbas")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("NT-Screening array job file and records in batches are produced", {
  tempSaveDir <- withr::local_tempdir()
  
  screeningSelectedBatchesSlurm(
    msrawfileIndex=testIndexName, 
    batchDirs=file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol"), 
    saveDirectory=tempSaveDir,
    email="testEmail@test.de",
    screeningType = "nts"
  )
  
  expect_length(list.files(tempSaveDir),3)
  linesJobFile <- readLines(file.path(tempSaveDir, "arrayScreening.sbatch"))
  expect_match(linesJobFile[56], "nts")
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Screening for non-existent batches returns a helpful error", {
  expect_snapshot(try(screeningSelectedBatches(testIndexName, "foo", "bar")))
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

