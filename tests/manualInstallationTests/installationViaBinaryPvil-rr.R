

devtools::build(path = file.path(Sys.getenv("RENV_PATHS_CELLAR"), "ntsportal", glue::glue("ntsportal_{desc::desc_get_version()}.tar.gz")), binary = TRUE)

renv::deactivate()
install.packages("/beegfs/nts/renv_package_cellar/ntsportal/ntsportal_25.1.tar.gz", lib = "~/R/4.4.3test/")
.libPaths("~/R/4.4.3test/")
library(ntsportal)
renv::activate()
