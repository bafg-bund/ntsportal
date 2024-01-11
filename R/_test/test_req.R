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