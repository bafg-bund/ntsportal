#renv::deactivate()

td <- file.path(tempdir(), "binaryInstallationTest")
dir.create(td, recursive = T)
binaryDir <- file.path(td, "binary")
libraryPath <- file.path(td, "testLibrary")
dir.create(binaryDir)
dir.create(libraryPath)
binaryPath <- file.path(binaryDir, glue::glue("ntsportal_{desc::desc_get_version()}.tar.gz"))
devtools::build(path = binaryPath, binary = TRUE)
install.packages(binaryPath, lib = libraryPath)
filesInInstallation <- list.files(file.path(libraryPath, "ntsportal"))
stopifnot(
  "extdata" %in% filesInInstallation,
  "R" %in% filesInInstallation
)
oldPaths <- .libPaths()
withr::with_libpaths(c(libraryPath, oldPaths), {
  library(ntsportal)
  connectNtsportal()
})
unlink(td, recursive = T)

rm(list = ls())
#renv::activate()
