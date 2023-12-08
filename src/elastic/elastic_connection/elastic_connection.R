

x_es_fun_connect_to_elastic <- function() {
  
  ec <- config::get(config = Sys.getenv("R_CONFIG_ACTIVE", "bafg_elastic_connect"))
  es_conn <- connect(transport_schema=ec$transport_schema, 
                     headers = list(Authorization = ec$authorization),
                     host=ec$host,
                     port=ec$port,
                     ssl_verifypeer = FALSE,
                     ssl_verifyhost = FALSE)
  return(es_conn)
  
}











# if (es_conn$ping()$cluster_name != "bfg-elastic-cluster") {
#   stop("Connection to es-db not established")
# }
# 
# rm(config_path)
# rm(ec)
# 
# logger::log_info("Connection to ntsp successful")


