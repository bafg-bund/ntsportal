index <- "g2_nts*"
path_ufid_db <- "ufid1.sqlite"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

ntsportal::es_test_fpfn(escon, udb, index)
DBI::dbDisconnect(udb)
