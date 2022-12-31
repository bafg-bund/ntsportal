# script to clear previous contents of ufid db on elasticsearch

index <- "g2_ufid1"

config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = 'elastic-mn-01.hpc.bafg.de', port = 9200, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https")

elastic::docs_delete_by_query(escon, index, body = '
                              {
                                "query": {
                                  "match_all": {}
                                }
                              }
                              ')

message("done clearing ufid db on elasticsearch on ", date())
