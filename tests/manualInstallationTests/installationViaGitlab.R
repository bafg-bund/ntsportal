

renv::deactivate()
.libPaths("~/R/450test")
.libPaths()
remotes::install_gitlab(
  repo = "nts/ntsportal@main",
  host = "https://gitlab.lan.bafg.de",
  lib = "~/R/450test"
)
library(ntsportal)
renv::activate()
