

# To create the new msrawfiles version, use python.




createAllMsrawfileRecords <- function() {
  allRecords <- getAllMsrawfilesRecords(testIndexName)
  allBatches <- splitRecordsByDir(allRecords)
  saveRDS(allBatches, test_path("fixtures", "screening-fileScanning", "allMsrawfileRecords.RDS"))
}
createAllMsrawfileRecords()

