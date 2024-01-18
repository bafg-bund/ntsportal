#index_list <- cat_indices(conn = es_conn, parse = TRUE)

#test_indeces <- Search(es_conn, index = c(index_list[7]), size = 10, asdf = TRUE)

#test_indeces <- Search(es_conn, index = "g2_nts_bfg", size = 10000, asdf = TRUE)
#test_df <- as.data.table(test_indeces$hits$hits) 
#%>% 
#  select(!c("_source.ms2","_source.eic", "_source.ms1"))


#name_list_diff_indeces <- list()
#name_list_diff_indeces[[1]] <- names(test_df)
#name_list_diff_indeces[[2]] <- names(test_df)
#name_list_diff_indeces[[3]] <- names(test_df)
#name_list_diff_indeces[[4]] <- names(test_df)
#name_list_diff_indeces[[5]] <- names(test_df)
#name_list_diff_indeces[[6]] <- names(test_df)
#name_list_diff_indeces[[7]] <- names(test_df)
#write.csv(xxx,"./name_list.txt")

test_indeces_1 <- Search(es_conn, index = c(index_list[1]), size = 100, asdf = TRUE)
test_df_1 <- as.data.table(test_indeces_1$hits$hits)
test_indeces_2 <- Search(es_conn, index = c(index_list[2]), size = 100, asdf = TRUE)
test_df_2 <- as.data.table(test_indeces_2$hits$hits)
test_indeces_3 <- Search(es_conn, index = c(index_list[3]), size = 100, asdf = TRUE)
test_df_3 <- as.data.table(test_indeces_3$hits$hits)
test_indeces_4 <- Search(es_conn, index = c(index_list[4]), size = 100, asdf = TRUE)
test_df_4 <- as.data.table(test_indeces_4$hits$hits)
test_indeces_5 <- Search(es_conn, index = c(index_list[5]), size = 100, asdf = TRUE)
test_df_5 <- as.data.table(test_indeces_5$hits$hits)
test_indeces_6 <- Search(es_conn, index = c(index_list[6]), size = 100, asdf = TRUE)
test_df_6 <- as.data.table(test_indeces_6$hits$hits)
test_indeces_7 <- Search(es_conn, index = c(index_list[7]), size = 100, asdf = TRUE)
test_df_7 <- as.data.table(test_indeces_7$hits$hits)

data <- rbindlist(list(test_df_1,
                       test_df_2,
                       test_df_3, 
                       test_df_4,
                       test_df_5,
                       test_df_6,
                       test_df_7),
                  fill = TRUE,
                  use.names = TRUE)










#09.01.24



# #temp_data_for_demo <- func_get_demo_data
# es_glob_df <- func_get_demo_data
#   #reactive({temp_data_for_demo})
# print(es_glob_df)
# 
# observeEvent(get_json_query_1(),{
#   print("action get data")
#   es_glob_df <- x_es_fun_get_data_from_elastic(index_list = input$in_req_index, body = get_json_query_1()) #(index_list = input$in_req_index)
#   print(es_glob_df)
# })

# es_glob_df <- eventReactive(input$request_filtered_data,{
#   print("action get data")
#   data <- x_es_fun_get_data_from_elastic(index_list = input$in_req_index)#(index_list = input$in_req_index)
#   #print(data)
#   return(data)
# })
# 





#  
# get_data <- eventReactive(input$request_filtered_data,{
#   #print("go")
#   data <<- x_es_fun_get_data_from_elastic(index_list = input$in_req_index)
#   #print(data)
#   return(data)
# })
# 

#es_glob_dfs <- reactiveValues(es_df_data_tab = NULL) 

# 

# es_globe_df <- reactive({
#   es_globe_df <- get_json_query()
#   return(es_globe_df)
# })

# observe({
#   print(get_json_query())
#   es_glob_df <<- get_json_query()
# })


#18.01.24



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



# func_get_parameters <- function(){
#   temp_data <- x_es_fun_get_data_from_elastic(index_list = unlist(x_es_fun_list_indices()$index)) #as.data.table(fromJSON("./Data/cbz_cand.json"))
#   temp_data <- temp_data %>% unnest(X_source.rtt)
#   param_data <- list()
#   param_data$station <- list(na.omit(unique(temp_data$X_source.station)))
#   param_data$river <- list(na.omit(unique(temp_data$X_source.river)))
#   param_data$matrix <- list(na.omit(unique(temp_data$X_source.matrix)))
#   param_data$tag <- list(na.omit(unique(temp_data$X_source.tag)))
#   param_data$comp_group <- list(na.omit(unique(temp_data$X_source.comp_group)))
#   param_data$rtt_method <- list(na.omit(unique(temp_data$method)))
#   param_data$name <- list(na.omit(unique(temp_data$X_source.name)))
#   param_data$ufid <- list(na.omit(unique(temp_data$X_source.ufid)))
#   
#   param_data$rt_min_max <- c(min(temp_data$rt), max(temp_data$rt))
#   param_data$mz_min_max <- c(min(temp_data$X_source.mz), max(temp_data$X_source.mz))
#   
#   rm(temp_data)
#   return(param_data)
# }




# func_get_demo_data_dash <- function(){
#   dash_data <- as.data.table(fromJSON("./Data/cbz_cand.json")) %>%
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




##-------------------------------------config

##-------------------------------------getdata

# json_data <- fromJSON("./Data/cbz_cand.json")
# #test_data <- json_data
# test_data <- as.data.table(json_data)
# test_data <- test_data %>% unnest(X_source.rtt)
# test_data <- test_data %>% unnest(X_source.eic)



##-------------------------------------dashboard-data
# dashboard_data <- as.data.table(json_data)
##-------------------------------------filter dashboard
#TO DO na handling, in df

# func_get_dashboard_data <- function(){
#   dash_data <- x_es_fun_get_data_from_elastic(index_list = unlist(x_es_fun_list_indices()$index), size = 100) %>% #as.data.table(fromJSON("./Data/cbz_cand.json")) %>% 
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
#               )) %>% 
#     unnest(X_source.rtt) %>%
#     rename(tRet=rt,
#            Method=method) %>%
#     mutate(location=paste0(lat,lon))
#   return(dash_data)
#   }

##-------------------------------------request data

# func_get_index <- function(){
#   index_data <- list()
#   index_data$index <- list(na.omit(unique(as.data.table(fromJSON("./Data/cbz_cand.json"))$X_index)))
#   index_data$source <- list(na.omit(unique(as.data.table(fromJSON("./Data/cbz_cand.json"))$X_source.data_source)))
#   return(index_data)
# }


##-------------------------------------data data


#x_es_fun_get_data_from_elastic(index_list = unlist(x_es_fun_list_indices()$index)) %>% #
# func_get_data_data <- function(){
#   data_data <- as.data.table(fromJSON("./Data/cbz_cand.json")) %>% 
#     select(!c("X_source.ms2","X_source.eic", "X_source.ms1")) %>%
#     unnest(X_source.rtt) %>%
#     rename(tRet=rt,
#            Method=method)
#   return(data_data)
# }