

tempSaveDir <- withr::local_tempdir()
realSaveDir <- test_path("fixtures", "screening-ingest", "exampleJson")

batchDirectory <- file.path(rootDirectoryForTestMsrawfiles, "olmesartan-d6-bisoprolol")
pathJson <- dbaScreeningSelectedBatches(testIndexName, batchDirectory, tempSaveDir)

file.remove(list.files(realSaveDir, full.names = T))
file.copy(pathJson, test_path("fixtures", "screening-ingest", "exampleJson"))
