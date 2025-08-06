
#' Create images of structural formulae from the spectral library
#' @description Uses the spectral library (CSL) to obtain SMILES codes
#' to create PNG images of the structural formulae for all compounds.
#' @param databasePath Path to spectral library (CSL, SQLite format)
#' @param targetDir Path where the PNGs are to be saved
#' @export
createAllStructures <- function(databasePath, targetDir) {
  tempDir <- withr::local_tempdir()
  newDbPath <- file.path(tempDir, "db.db")
  importCsl(databasePath, newDbPath)
  compList <- extractCompoundList(newDbPath)
  cli_progress_bar("Creating PNGs", total = nrow(compList))
  for (i in 1:nrow(compList)) {
    createPngFromSmiles(compList[i, "SMILES"], compList[i, "inchikey"], targetDir)
    cli_progress_update()
  }
  cli_process_done()
  
  file.remove(list.files(tempDir, full.names = TRUE))
  file.remove(tempDir)
  message("Complete")
}

extractCompoundList <- function(databaseFile) {
  database <- RSQLite::dbConnect(RSQLite::dbDriver("SQLite"), dbname = databaseFile)
  compoundList <- RSQLite::dbReadTable(database, "compound")
  RSQLite::dbDisconnect(database)
  compoundList
}

createPngFromSmiles <- function(smiles,inchikey,targetPath) {
  iAtomContainer <- rcdk::parse.smiles(smiles, kekulise = TRUE)
  structureMatrix <- lapply(iAtomContainer,makeStructureMatrix)
  outputPath <- file.path(targetPath, paste0(inchikey,".png"))
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

importCsl <- function(databasePath, targetPath) {
  file.copy(
    from = databasePath,
    to = targetPath,
    copy.date = TRUE,
    overwrite = TRUE
  )
}

#' Check that all structural formula PNGs have been created
#' @description Checks that PNGs for structural formulae are 
#' consistent with the compounds in the spectral library. It
#' compares the InChI-Keys in the library and filenames and
#' prints any inconsistencies to the console.  
#' @inheritParams createAllStructures
#' @export
checkPngAvailability <- function(databasePath, targetDir) {
  compList <- extractCompoundList(databasePath)
  missingPngs <- getMissingPngs(databasePath, targetDir)
  redundantPngs <- getRedundantPngs(databasePath, targetDir)
  
  if (length(missingPngs) > 0) {
    cli_alert_warning("Structure PNG(s) missing")
    compIds <- paste(sapply(missingPngs, \(x) compList[compList$inchikey == x, "name"]), missingPngs, sep = " - ")
    cli_ul(compIds)
  }
  if (length(redundantPngs) > 0) {
    cli_alert_warning("Structure PNG(s) have no corresponding DB entry")
    cli_ul(redundantPngs)
  }
}

getRedundantPngs <- function(databasePath, targetDir) {
  compList <- extractCompoundList(databasePath)
  inchikeyFiles <- getAllPngInchikeys(targetDir)
  setdiff(inchikeyFiles, compList$inchikey)
}

getMissingPngs <- function(databasePath, targetDir) {
  compList <- extractCompoundList(databasePath)
  inchikeyFiles <- getAllPngInchikeys(targetDir)
  setdiff(compList$inchikey, inchikeyFiles)
}

getAllPngInchikeys <- function(targetDir) {
  allFiles <- list.files(targetDir, pattern = "\\.png$")
  stringr::str_match(allFiles, "^(.*)\\.png$")[,2]
}

#' Delete redundant PNGs
#' @description Remove PNGs for compounds not found in the library
#' @inheritParams createAllStructures
#' @export
removeRedundantPngs <- function(databasePath, targetDir) {
  redundantPngs <- getRedundantPngs(databasePath, targetDir)
  for (ik in redundantPngs) {
    file.remove(file.path(targetDir, paste0(ik, ".png")))
    cli_alert_info("Removed file {ik}.png")
  }
}

#' Check synchronization with the picture server
#' @description Checks that a PNG file on the picture server and on disk are identical.
#' @param fileUrl URL of the file on the picture server
#' @param filePathHd Path to the file on disk
#' @export
checkPngSyncronization <- function(fileUrl, filePathHd) {
  saveDir <- withr::local_tempdir()
  download.file(fileUrl, file.path(saveDir, "urlFile.png"), method = "curl", extra = c("--insecure"), quiet = T)
  file.copy(filePathHd, file.path(saveDir, "fileOnHd.png"))
  urlImg <- magick::image_read(file.path(saveDir, "urlFile.png"))
  hdImg <- magick::image_read(file.path(saveDir, "fileOnHd.png"))
  urlImgPx <- as.integer(urlImg[[1]]) # Assuming a single frame
  hdImgPx <- as.integer(hdImg[[1]])
  if (all(urlImgPx == hdImgPx)) {
    cli::cli_alert_success("Images are the same")
  } else {
    cli::cli_alert_warning("Images are not the same")
  }
}