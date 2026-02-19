

newLib <- tempfile()
dir.create(newLib)
oldPaths <- .libPaths()
remotes::install_github("bafg-bund/ntsworkflow", lib = newLib)
remotes::install_github("bafg-bund/ntsportal", lib = newLib)
.libPaths(newLib)

library(ntsportal)
.libPaths(oldPaths[1])
.libPaths()
unlink(newLib, recursive = TRUE)

