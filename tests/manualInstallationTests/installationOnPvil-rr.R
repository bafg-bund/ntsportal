

tmpDir <- "~/temp_installation_ntsportal"
dir.create(tmpDir)
setwd(tmpDir)
system("git clone https://gitlab.lan.bafg.de/nts/ntsportal.git")
rstudioapi::openProject(file.path(tmpDir, "ntsportal"))

# when in new project, run
renv::restore()  # activate project
renv::install("bafg-bund/ntsworkflow")
devtools::load_all()
testthat::test_file("tests/testthat/test-screening-fileScanning.R")

# switch back to original project, cleanup

tmpDir <- "~/temp_installation_ntsportal"
file.remove(list.files(tmpDir, recursive = T))
file.remove(tmpDir)
