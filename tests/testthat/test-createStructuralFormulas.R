test_that("copy CSL", {
  tempSaveDir <- withr::local_tempdir()
  importCsl("/srv/cifs-mounts/g2/G/G2/HRMS/Spektrendatenbank/sqlite/CSL_v24.4.db",tempSaveDir)
  expect_true(file.exists(file.path(tempSaveDir,"CSL_v24.4.db")))
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})

test_that("generate structural formula for diclofenac", {
  tempSaveDir <- withr::local_tempdir()
  name <- createPngFromSmiles(smilesCode = "C1=CC=C(C(=C1)CC(=O)O)NC2=C(C=CC=C2Cl)Cl", inchikey = "DCOPUUMXTXDBNB-UHFFFAOYSA-N",targetPath = tempSaveDir)
  expect_true(file.exists(file.path(name)))
  file.show(file.path(name))
  file.remove(list.files(tempSaveDir,full.names = TRUE))
  file.remove(tempSaveDir)
})