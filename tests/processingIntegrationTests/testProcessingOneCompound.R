

# first get records a sample and a blank
compoundToTest <- "Propafenone"
measFileDir <- rstudioapi::selectDirectory()
measFileDir <- normalizePath(measFileDir)
filesOfInterest <- list.files(measFileDir)  # Use pattern to choose specific files.

allRecords <- getSelectedMsrawfileBatches("ntsp_msrawfiles", measFileDir)
selectedRecords <- list()
for (rec in allRecords[[1]]) {
  if (rec$filename %in% filesOfInterest || rec$blank)
    selectedRecords <- c(selectedRecords, list(rec))
}

results <- scanBatchDbas(records = selectedRecords, compsToProcess = compoundToTest)
View(results$peakList)
View(results$reintegrationResults)


featureRecordsBatch <- convertToRecord(results, selectedRecords)

saveDirectory <- withr::local_tempdir()
saveRecord(featureRecordsBatch, saveDirectory)


# Ingest results
# ingestJson(saveDirectory)
