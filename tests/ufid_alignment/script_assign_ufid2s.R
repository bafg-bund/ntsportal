
# process all indices to assign ufid2s to all features

index <- "g2_nts*"
config_path <- "~/config.yml"
path_ufid_db <- "~/sqlite_local/ufid1.sqlite"

ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = 'elastic-mn-01.hpc.bafg.de', port = 9200, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https")

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
ntsportal::ufid2_alignment(escon, index, rtTol = 0.5, mzTolmDa = 5, minPoints = 5)

# run tests (ufid and ufid2)
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
ntsportal::es_test_fpfn(escon, udb, index)
DBI::dbDisconnect(udb)

