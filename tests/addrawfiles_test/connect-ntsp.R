

config_path <- "~/Projekte/screening/config.yml"

ec <- config::get("elastic_connect", file = config_path)

escon <- elastic::connect(
  host = 'elastic.dmz.bafg.de', 
  port = 443, user=ec$user, 
  pwd  = ec$pwd,
  transport_schema = "https"
)

if (escon$ping()$cluster_name != "bfg-elastic-cluster") {
  stop("Connection to es-db not established")
}

rm(config_path)
rm(ec)

logger::log_info("Connection to ntsp successful")

