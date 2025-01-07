

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


pathInIndex <- function(testPath, rfIndex) {
  res <- elastic::Search(
    get("escon", envir = global_env()), rfIndex, size = 0, 
    body = list(query = list(term = list(path = testPath)))
  )
  res$hits$total$value == 1
}
