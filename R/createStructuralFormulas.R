
#' Create images of structural formulas for display in Kibana dashboard
#'
#' @param databasePath Path to CSL
#' @param targetDir Path to save png of structures
#'
#' @return no return value
#' @export
#'
createAllStructures <- function(databasePath, targetDir) {
  tempDir <- withr::local_tempdir()
  newDbPath <- file.path(tempDir, "db.db")
  importCsl(databasePath, newDbPath)
  compList <- extractCompoundList(newDbPath)
  
  for (i in 1:nrow(compList))
    createPngFromSmiles(compList[i, "SMILES"], compList[i, "inchikey"], targetDir)
  
  file.remove(list.files(tempDir,full.names = TRUE))
  file.remove(tempDir)
  message("Complete")
}

extractCompoundList <- function(databaseFile) {
  sqLiteDriver <- RSQLite::dbDriver("SQLite")
  database <- RSQLite::dbConnect(RSQLite::dbDriver("SQLite"), dbname = databaseFile)
  RSQLite::dbListTables(database)
  compoundList <- RSQLite::dbReadTable(database, "compound")
  RSQLite::dbDisconnect(database)
  compoundList
}

createPngFromSmiles <- function(smiles,inchikey,targetPath) {
  iAtomContainer <- rcdk::parse.smiles(smiles, kekulise = TRUE)
  structureMatrix <- lapply(iAtomContainer,makeStructureMatrix)
  outputPath <- file.path(targetPath,paste0(inchikey,".png"))
  png(outputPath)
  plot.new()
  plot.window(c(0, 1), c(0, 1))
  rasterImage(structureMatrix[[1]], 0, 0, 1, 1)
  dev.off()
  outputPath
}

makeStructureMatrix <- function(structureObject) {
  # set resolution of the image
  resolution <- rcdk::get.depictor(width = 400, height = 400, zoom = 2) 
  rcdk::view.image.2d(molecule = structureObject, depictor = resolution)
}

importCsl <- function(databasePath,targetPath) {
  file.copy(
    from = databasePath,
    to = targetPath,
    copy.date = TRUE,
    overwrite = TRUE
  )
}