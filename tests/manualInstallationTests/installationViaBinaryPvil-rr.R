

devtools::build(path = file.path(Sys.getenv("RENV_PATHS_CELLAR"), "ntsportal", glue::glue("ntsportal_{desc::desc_get_version()}.tar.gz")), binary = TRUE)

renv::deactivate()
install.packages("/beegfs/nts/renv_package_cellar/ntsportal/ntsportal_25.1.tar.gz", lib = "~/R/450test")
install.packages("/beegfs/nts/renv_package_cellar/ntsworkflow/ntsworkflow_0.2.6.tar.gz", lib = "~/R/450test")
.libPaths("~/R/450test")
library(ntsportal)
renv::activate()
