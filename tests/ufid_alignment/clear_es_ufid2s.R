# clear all ufids in es and in ufid_db


index <- "g2_nts*"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)


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

message("All ufid2 removed at ", date())
