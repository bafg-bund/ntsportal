
test_that("nt-screening for a small batch produces an RDS with peaks", {
  tempSaveDir <- withr::local_tempdir()
  batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
  startTime <- Sys.time()
  pathRds <- screeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir, screeningType = "nts")
  endTime <- Sys.time()
  message("Time needed to processes batch nts: ", round(difftime(endTime, startTime, units = "secs")), " s")
  expect_length(list.files(tempSaveDir), 1)
  featureRecs <- readRDS(pathRds)
  expect_true(all(map_lgl(featureRecs, \(x) fieldsExist(c("mz", "rt", "path", "area", "area_internal_standard", "eic"), x))))
  expect_false("name" %in% unique(list_c(map(featureRecs, names))))
  expect_true(all(map_lgl(featureRecs, \(x) is.null(x$name))))
  expect_true(all(map_lgl(featureRecs, \(x) x$mz > 100)))
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
  
  oneMsrawfilesBatch <- readRDS(file.path(tempSaveDir, "recordsInBatches.RDS"))[[1]]
  expect_s3_class(oneMsrawfilesBatch, "dbasMsrawfilesBatch")
  oneMsrawfilesRec <- oneMsrawfilesBatch[[1]]
  expect_s3_class(oneMsrawfilesRec, "dbasMsrawfilesRecord")
  expect_contains(names(oneMsrawfilesRec), c("chrom_method", "dbas_area_threshold"))
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("NT-Screening array job file and records in batches are produced", {
  tempSaveDir <- withr::local_tempdir()
  testEmail <- "testEmail@test.de"
  screeningSelectedBatchesSlurm(
    msrawfileIndex=testIndexName, 
    batchDirs=file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol"), 
    saveDirectory=tempSaveDir,
    email=testEmail,
    screeningType = "nts"
  )
  
  expect_length(list.files(tempSaveDir),3)
  linesJobFile <- readLines(file.path(tempSaveDir, "arrayScreening.sbatch"))
  expect_equal(sum(grepl(tempSaveDir, linesJobFile)), 3)
  expect_equal(sum(grepl("screeningSlurm.R", linesJobFile)), 1)
  expect_equal(sum(grepl(testEmail, linesJobFile)), 1)
  
  oneMsrawfilesBatch <- readRDS(file.path(tempSaveDir, "recordsInBatches.RDS"))[[1]]
  expect_s3_class(oneMsrawfilesBatch, "ntsMsrawfilesBatch")
  oneMsrawfilesRec <- oneMsrawfilesBatch[[1]]
  expect_s3_class(oneMsrawfilesRec, "ntsMsrawfilesRecord")
  expect_contains(names(oneMsrawfilesRec), c("chrom_method", "nts_mz_step"))
  
  file.remove(list.files(tempSaveDir, full.names = T))
  file.remove(tempSaveDir)
})

test_that("Screening for non-existent batches returns a helpful error", {
  tempSaveDir <- "~/tmp/temp-dir-ntsportal-test-screening-launchProcess"
  if (dir.exists(tempSaveDir))
    unlink(tempSaveDir, recursive = T)
  expect_snapshot(try(screeningSelectedBatches(testIndexName, "foo", tempSaveDir)))
})

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

