
test_that("All structures are created", {
  tempSaveDir <- withr::local_tempdir()
  dbPath <- test_path("fixtures", "createStructuralFormulas", "CSL_olmesartan-d6.db")
  compList <- extractCompoundList(dbPath)
  createAllStructures(dbPath, tempSaveDir)
  expect_equal(nrow(compList), length(list.files(tempSaveDir, pattern = "\\.png$")))
  
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("generate structural formula for diclofenac", {
  tempSaveDir <- withr::local_tempdir()
  structurePng <- createPngFromSmiles(smiles = "C1=CC=C(C(=C1)CC(=O)O)NC2=C(C=CC=C2Cl)Cl", inchikey = "DCOPUUMXTXDBNB-UHFFFAOYSA-N",targetPath = tempSaveDir)
  expect_true(file.exists(file.path(structurePng)))
  file.show(file.path(structurePng))
  file.remove(list.files(tempSaveDir,full.names = TRUE))
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




