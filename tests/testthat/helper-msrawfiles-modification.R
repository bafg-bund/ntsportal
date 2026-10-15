

makeTestRecord <- function(originalPath, indexName, saveDir) {
  templateIdFromMsrawfilesIndex <- "VNSTWpABQ5NoSyLHKzdl"
  suppressMessages(
    idAdded <- addRawfiles(
      rfIndex = indexName, 
      templateId = templateIdFromMsrawfilesIndex,
      newPaths = originalPath, 
      dirMeasurmentFiles = test_path("fixtures", "msrawfiles-addRecord"),
      prompt = F,
      saveDirectory = saveDir
    )  
  )
  idAdded
}


