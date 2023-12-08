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
