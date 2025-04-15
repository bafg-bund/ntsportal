

tmpDir <- "~/temp_installation_ntsportal"
dir.create(tmpDir)
setwd(tmpDir)
system("git clone https://gitlab.lan.bafg.de/nts/ntsportal.git")
rstudioapi::openProject(file.path(tmpDir, "ntsportal"))

# when in new project, run
renv::restore()  # activate project
renv::install("bafg-bund/ntsworkflow")

# create new python virtual env
reticulate::virtualenv_create("testvenv", requirements = "requirements.txt")
oldPythonEnv <- Sys.getenv("RETICULATE_PYTHON")
newPythonEnv <- "~/.virtualenvs/testvenv/bin/python"
Sys.setenv(RETICULATE_PYTHON = newPythonEnv)
reticulate::use_virtualenv("~/.virtualenvs/testvenv")
stopifnot(grepl("testvenv", reticulate::py_config()$python))

devtools::load_all()
testthat::test_file("tests/testthat/test-screening-fileScanning.R")
testthat::test_file("tests/testthat/test-connectionToElasticSearch.R")


# switch back to original project, cleanup

reticulate::virtualenv_remove("~/.virtualenvs/testvenv")
tmpDir <- "~/temp_installation_ntsportal"
system2("rm", c("-rf", tmpDir))
