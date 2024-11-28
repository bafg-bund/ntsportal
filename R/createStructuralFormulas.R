# Create a copy of the database:
importCsl <- function(databasePath,targetPath) {
  file.copy(
    from = databasePath,
    to = targetPath,
    copy.date = TRUE,
    overwrite = TRUE
  )
}

createPngFromSmiles <- function(smiles,inchikey,targetPath) {
  structureObject1 <- rcdk::parse.smiles(smiles, kekulise = TRUE)
  structureMatrix <- makeStructureMatrix(structureObject1)
  outputPath <- file.path(targetPath,paste0(inchikey,".png"))
  png(outputPath)
  plot.new()
  plot.window(c(0, 1), c(0, 1))
  rasterImage(structureMatrix, 0, 0, 1, 1)
  dev.off()
  outputPath
}

makeStructureMatrix <- function(structureObject) {
  # set resolution of the image
  resolution <- get.depictor(width = 400, height = 400, zoom = 2) 
  view.image.2d(structureObject, depictor = resolution)
}