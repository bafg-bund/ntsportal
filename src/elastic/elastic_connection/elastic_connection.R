
ec <- config::get(config = Sys.getenv("R_CONFIG_ACTIVE", "elastic_connect"))

es_conn <- elastic::connect(
  host = 'elastic.bafg.de',  #'elastic.dmz.bafg.de'
  port = 443, 
  user=ec$user,
  pwd  = ec$pwd,
  transport_schema = "https"
)

if (es_conn$ping()$cluster_name != "bfg-elastic-cluster") {
  stop("Connection to es-db not established")
}

rm(config_path)
rm(ec)

logger::log_info("Connection to ntsp successful")

Search(es_conn, index = "g2_nts_v1_bfg", time_scroll = "5m")
