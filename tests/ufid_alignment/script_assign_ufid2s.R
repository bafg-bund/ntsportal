
# process all indices to assign ufid2s to all features

index <- "g2_nts*"
config_path <- "~/config.yml"
path_ufid_db <- "~/HRMS_Z/sw_entwicklung/ntsportal/ufid1.sqlite"

ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

# first clear current ufid2s
message("Clearing previous ufid2s at ", date())
elastic::docs_update_by_query(escon, index, body =
                                '
{
  "query": {
    "exists": {
      "field": "ufid2"
    }
  },
  "script": {
    "source": "ctx._source.remove(\'ufid2\')",
    "lang": "painless"
  }
}
'
)

# ufid2 alignment
ntsportal::ufid2_alignment(escon, index, rtTol = 0.3, mzTolmDa = 5, minPoints = 5)

# run tests (ufid and ufid2)
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
ntsportal::es_test_fpfn(escon, udb, index)
DBI::dbDisconnect(udb)

