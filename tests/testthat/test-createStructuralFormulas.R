test_that("copy CSL", {
  tempSaveDir <- withr::local_tempdir()
  importCsl("/srv/cifs-mounts/g2/G/G2/HRMS/Spektrendatenbank/sqlite/CSL_v24.4.db",tempSaveDir)
  expect_true(file.exists(file.path(tempSaveDir,"CSL_v24.4.db")))
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("generate structural formula for diclofenac", {
  tempSaveDir <- withr::local_tempdir()
  name <- createPngFromSmiles("OC(Cc1c(Nc2c(Cl)cccc2Cl)cccc1)=O","DCOPUUMXTXDBNB-UHFFFAOYSA-N",tempSaveDir)
  expect_true(file.exists(file.path(tempSaveDir, name)))
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})
#Testen, ob es für mehrere Substanzen funktioniert oder ob Smiles-Code nicht stimmt

test_that("generate structure matrix", {
  library(rcdk)
  library(rJava)
  testmol <- readRDS(test_path("fixtures","createStructuralFormulas","testmol.RDS"))
  makeStructureMatrix(testmol)
})
