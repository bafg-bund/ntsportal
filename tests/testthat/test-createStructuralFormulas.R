
test_that("All structures are created", {
  tempSaveDir <- withr::local_tempdir()
  dbPath <- test_path("fixtures", "CSL_olmesartan-d6.db")
  compList <- extractCompoundList(dbPath)
  createAllStructures(dbPath, tempSaveDir)
  expect_equal(nrow(compList), length(list.files(tempSaveDir, pattern = "\\.png$")))
  
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("generate structural formula for diclofenac", {
  tempSaveDir <- withr::local_tempdir()
  expect_snapshot_file(
    pngPath <- createPngFromSmiles(
      smiles = "C1=CC=C(C(=C1)CC(=O)O)NC2=C(C=CC=C2Cl)Cl", 
      inchikey = "DCOPUUMXTXDBNB-UHFFFAOYSA-N",
      targetPath = tempSaveDir
    ),
    "DCOPUUMXTXDBNB-UHFFFAOYSA-N.png"
  )
  
  expect_true(file.exists(pngPath))
  #file.show(pngPath)
  file.remove(list.files(tempSaveDir, full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("Generate structural formula for (2-dodecanoylamino-ethyl)-dimethyl-tetradecyl-ammonium", {
  tempSaveDir <- withr::local_tempdir()
  
  expect_snapshot_file(
    pngPath <- createPngFromSmiles(
      smiles = "CCCCCCCCCCCCCC[N+](C)(C)CCNC(=O)CCCCCCCCCCC", 
      inchikey = "FFBIRENCNBBTSF-UHFFFAOYSA-O",
      targetPath = tempSaveDir
    ),
    "FFBIRENCNBBTSF-UHFFFAOYSA-O.png"
  )
  expect_true(file.exists(pngPath))
  #file.copy(pngPath, "/srv/cifs-mounts/ntsportal/intern/picture_sync/FFBIRENCNBBTSF-UHFFFAOYSA-O.png", overwrite = T)
  file.remove(list.files(tempSaveDir, full.names = TRUE))
  file.remove(tempSaveDir)
  
})

test_that("extract compound list from database", {
  tempSaveDir <- withr::local_tempdir()
  importCsl(databasePath = "/srv/cifs-mounts/g2/G/G2/HRMS/Spektrendatenbank/sqlite/CSL_v24.4.db",targetPath = tempSaveDir)
  
  compList <- extractCompoundList(databaseFile = file.path(tempSaveDir,"CSL_v24.4.db"))
  expect_length(compList, 8)
  expect_gt(nrow(compList), 2)
  
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("copy CSL", {
  tempSaveDir <- withr::local_tempdir()
  importCsl("/srv/cifs-mounts/g2/G/G2/HRMS/Spektrendatenbank/sqlite/CSL_v24.4.db",tempSaveDir)
  expect_true(file.exists(file.path(tempSaveDir,"CSL_v24.4.db")))
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})


test_that("Check if missing PNGs are reported", {
  tempSaveDir <- withr::local_tempdir()
  dbPath <- test_path("fixtures", "CSL_olmesartan-d6.db")
  
  expect_snapshot(checkPngAvailability(dbPath, tempSaveDir))
  # pthDb <- getSpectralLibraryPath("ntsp25.1_msrawfiles")
  # compList <- extractCompoundList(pthDb)
  # pthStructures <- "/srv/cifs-mounts/ntsportal/intern/picture_sync"
  
})


