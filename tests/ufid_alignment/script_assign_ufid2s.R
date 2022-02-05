
# process all indices to assign ufid2s to all features

index <- "g2_nts*"
config_path <- "~/config.yml"
path_ufid_db <- "~/HRMS_Z/sw_entwicklung/ntsportal/ufid1.sqlite"

ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

ntsportal::ufid2_alignment(escon, index, rtTol = 0.3, mzTolmDa = 5, minPoints = 5)


udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
ntsportal::es_test_fpfn(escon, udb, index)
DBI::dbDisconnect(udb)

