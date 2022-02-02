# script to clear previous contents of ufid db on elasticsearch

index <- "g2_ufid1"

config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

elastic::docs_delete_by_query(escon, index, body = '
                              {
                                "query": {
                                  "match_all": {}
                                }
                              }
                              ')

message("done clearing ufid db on elasticsearch on ", date())
