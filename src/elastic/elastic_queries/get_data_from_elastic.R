source("./src/elastic/elastic_connection/elastic_connection.R")

x_es_fun_get_data_from_elastic <- function(index_list=unlist(x_es_fun_list_indices(conn = x_es_fun_connect_to_elastic())$index), 
                                           conn=x_es_fun_connect_to_elastic(),
                                           size=10000,
                                           body=NULL){
  
  df_list <- list()
  for (i in 1:length(index_list)) {
    temp_es_data <- Search(conn, index = c(index_list[i]), size = size, asdf = TRUE, body = body)
    df_list[[i]] <- as.data.table(temp_es_data$hits$hits) 
  }
  data <- rbindlist(df_list, fill = TRUE) #, use.names = TRUE)
  colnames(data) <- paste0("X", colnames(data))
  return(data)
  
}



x_es_func_get_parameters <- function(index_list=NULL, 
                                     size=100, 
                                     date_start="2011-01-24",
                                     date_end="2023-01-24",
                                     data_source="bfg"){
  
  data_source <- toString(paste(data_source, collapse = '", "')) 
  
  if(is.null(index_list)){ index_list <- x_es_fun_list_indices(conn = x_es_fun_connect_to_elastic())$index }
  else{ index_list <- index_list }
  
  data_source <- toString(paste(data_source, collapse = '", "'))
  
  body_filter <- paste0(
    '{
      "_source": ["station", "river", "matrix", "tag", "comp_group", "rtt", "name", "ufid", "mz", "rt", "chrom_method"],
      "query": {
        "bool": {
          "must": [
            {
              "range": {
                "start": {
                  "gte": "',date_start,'" ,
                  "lte": "',date_end,'"
                }
              }
            },
            {
              "terms": {
                "data_source": ["',data_source,'"]
              }
            }
          ]
        }
      }
    }'
  )
  
  temp_data <- x_es_fun_get_data_from_elastic(index_list = unlist(index_list), size = size, body=body_filter)
  #temp_data <- temp_data %>% unnest(X_source.rtt)
  param_data <- list()
  param_data$station <- list(na.omit(unique(temp_data$X_source.station)))
  param_data$river <- list(na.omit(unique(temp_data$X_source.river)))
  param_data$matrix <- list(na.omit(unique(temp_data$X_source.matrix)))
  param_data$tag <- list(na.omit(unique(temp_data$X_source.tag)))
  param_data$comp_group <- list(na.omit(unique(unlist(temp_data$X_source.comp_group))))
  #param_data$rtt_method <- list(na.omit(unique(temp_data$method)))
  param_data$rtt_method <- list(na.omit(unique(temp_data$X_source.chrom_method)))
  param_data$name <- list(na.omit(unique(unlist(temp_data$X_source.name))))
  param_data$ufid <- list(na.omit(unique(temp_data$X_source.ufid)))
  
  param_data$rt_min_max <- c(min(temp_data$X_source.rt), max(temp_data$X_source.rt))
  param_data$mz_min_max <- c(min(temp_data$X_source.mz), max(temp_data$X_source.mz))
  
  param_data$idx <- unlist(index_list)
  
  rm(temp_data)
  return(param_data)
}



#data <- x_es_fun_get_data_from_elastic(index_list = unlist(x_es_fun_list_indices(conn = x_es_fun_connect_to_elastic())$index), 
#                                       conn = x_es_fun_connect_to_elastic())
#data <- x_es_fun_get_data_from_elastic(index_list = unlist(x_es_fun_list_indices()$index))


# x_es_func_get_dashboard_data <- function(index_list=NULL, 
#                                          size=1000, 
#                                          date_start="2011-01-24",
#                                          date_end="2023-01-24",
#                                          data_source=c("BfG","bfg","LANUV")){
#   
#   if(is.null(index_list)){ index_list <- x_es_fun_list_indices(conn = x_es_fun_connect_to_elastic())$index }
#   else{ index_list <- index_list }
#   
#   data_source <- toString(paste(data_source, collapse = '", "'))
#   
#   body_filter <- paste0(
#     '{
#       "_source": ["station", "river", "matrix", "tag", "comp_group", "rtt", "name", "ufid", "mz", "formula", "cas","intensity", "area", "chrom_method", "area_normalized", "loc", "start"],
#       "query": {
#         "bool": {
#           "must": [
#             {
#               "range": {
#                 "start": {
#                   "gte": "',date_start,'" ,
#                   "lte": "',date_end,'"
#                 }
#               }
#             },
#             {
#               "terms": {
#                 "data_source": ["',data_source,'"]
#               }
#             }
#           ]
#         }
#       }
#     }'
#   )
#   
#   
#   dash_data <- x_es_fun_get_data_from_elastic(index_list = unlist(index_list), size = size, body=body_filter) %>% #as.data.table(fromJSON("./Data/cbz_cand.json")) %>% 
#     select( c(Name=X_source.name, # if available
#               Formula=X_source.formula, 
#               CAS_RN=X_source.cas, 
#               Matrix=X_source.matrix, 
#               Intensity=X_source.intensity, 
#               X_source.rtt, # user needs to select rtt.method with Chrom. Method filter
#               Area=X_source.area, 
#               Chrom_Meth=X_source.chrom_method,
#               Ufid=X_source.ufid, # rm NA's , if available
#               #Ufid=X_source.river,
#               mz=X_source.mz, # average mz
#               Classification=X_source.comp_group, # rm NA's, as comma sep list
#               Area_normalized=X_source.area_normalized,
#               Stations=X_source.station,
#               lat=X_source.loc.lat,
#               lon=X_source.loc.lon,
#               Time=X_source.start,
#               River=X_source.river
#     )) %>% 
#     unnest(X_source.rtt) %>%
#     rename(tRet=rt,
#            Method=method) %>%
#     mutate(location=paste0(lat,lon))
#   return(dash_data)
# }




# for (i in 1:51146){
#   print(i) 
#   print(toString(unlist(xxx$Name[[i]])))
#   Sys.sleep(0.01)
#   }



#toString(xxx$Classification[[34513]])
#toString(unlist(xxx$Classification[[34513]]))




