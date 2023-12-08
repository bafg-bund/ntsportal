source("./src/elastic/elastic_connection/elastic_connection.R")

x_es_fun_list_indices <- function(conn=x_es_fun_connect_to_elastic()){ 
  
  body_filter <- paste0(
    '{
      "_source": "data_source",
      "query": {
        "match_all": {}
      }
    }'
  )
  
  index_data <- list()

  index_data$index <- list(cat_aliases(conn = conn, parse = TRUE)$V1)
  
  df_list <- list()
  for (i in 1:length(index_data$index[[1]])) {
    temp_es_data <- Search(conn, index = c(index_data$index[[1]][i]), size = 10000, asdf = TRUE, body = body_filter)
    df_list[[i]] <- as.data.table(temp_es_data$hits$hits) 
  }
  data <- rbindlist(df_list, fill = TRUE) #, use.names = TRUE)
  colnames(data) <- paste0("X", colnames(data))
  rm(df_list)
  

  index_data$source <- list(na.omit(unique(data$X_source.data_source)))
  rm(data)

  return(index_data)
}

#index_list <- x_es_fun_list_indices(conn = x_es_fun_connect_to_elastic())
#index_list$index
