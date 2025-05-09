
test_that("All structures are created", {
  tempSaveDir <- withr::local_tempdir()
  dbPath <- test_path("fixtures", "CSL_olmesartan-d6.db")
  compList <- extractCompoundList(dbPath)
  createAllStructures(dbPath, tempSaveDir)
  expect_equal(nrow(compList), length(list.files(tempSaveDir, pattern = "\\.png$")))
  
  file.remove(list.files(tempSaveDir, full.names = TRUE))
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
  file.remove(list.files(tempSaveDir, full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("Extracting compound list from database returns a data.frame", {
  tempSaveDir <- withr::local_tempdir()
  importCsl(databasePath = test_path("fixtures", "CSL_olmesartan-d6.db"), targetPath = tempSaveDir)
  
  compList <- extractCompoundList(databaseFile = file.path(tempSaveDir, "CSL_olmesartan-d6.db"))
  expect_length(compList, 8)
  expect_gte(nrow(compList), 1)
  
  file.remove(list.files(tempSaveDir, full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("Copy CSL works", {
  tempSaveDir <- withr::local_tempdir()
  importCsl(databasePath = test_path("fixtures", "CSL_olmesartan-d6.db"), targetPath = tempSaveDir)
  expect_true(file.exists(file.path(tempSaveDir, "CSL_olmesartan-d6.db")))
  file.remove(list.files(tempSaveDir, full.names = TRUE))
  file.remove(tempSaveDir)
})


test_that("Check for missing PNGs are reported correctly", {
  tempSaveDir <- withr::local_tempdir()
  dbPath <- test_path("fixtures", "CSL_olmesartan-d6.db")
  expect_snapshot(checkPngAvailability(dbPath, tempSaveDir))
  cat("test", file = file.path(tempSaveDir, "VTRAEEWXHOVJFV-XERRXZQWSA-N.png"))
  cat("test", file = file.path(tempSaveDir, "FFBIRENCNBBTSF-UHFFFAOYSA-O.png"))
  expect_snapshot(checkPngAvailability(dbPath, tempSaveDir))
  file.remove(list.files(tempSaveDir, full.names = T))
})


