importCsl <- function(databasePath,targetPath) {
  file.copy(
    from = databasePath,
    to = targetPath,
    copy.date = TRUE,
    overwrite = TRUE
  )
}

extractCompoundList <- function(databaseFile,targetPath) {
  sqLiteDriver <- RSQLite::dbDriver("SQLite")
  database <- RSQLite::dbConnect(RSQLite::dbDriver("SQLite"), dbname = databaseFile)
  RSQLite::dbListTables(database)
  compoundList <- RSQLite::dbReadTable(database, "compound")
  outputPath <- file.path(targetPath,paste0("compoundList.txt"))
  write.table(compoundList, file = outputPath)
  RSQLite::dbDisconnect(database)
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