# body-function 

# x_es_fun_body_time_source <- function(date_start="2011-01-24",
#                                       date_end="2023-01-24",
#                                       data_source="bfg"){
#   body_filter <- paste0(
#   '{
#       "_source": {        
#         "excludes": ["eic", "ms1", "ms2"]
#       },
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
#   return(body_filter)
# }




# x_es_fun_body_query_filter <- function(date_start="2011-01-24",
#                                        date_end="2023-01-24",
#                                        data_source="bfg",
#                                        station="rhein_ko_l",
#                                        river="rhine",
#                                        matrix="water",
#                                        tag="component_167394941953",
#                                        comp_group="*",
#                                        rtt_method=NULL,
#                                        name=NULL,
#                                        ufid=NULL,
#                                        rtt_rt_min=NULL,
#                                        rtt_rt_max=NULL,
#                                        mz_min=NULL,
#                                        mz_amx=NULL
#                                        ){
#   
#   
# }





x_es_fun_query_filter_func_elastic <- function(date_start="2011-01-24",
                                               date_end="2023-01-24",
                                               data_source="bfg",
                                               station="rhein_ko_l",
                                               river="rhine",
                                               matrix="water",
                                               tag="component_167394941953",
                                               comp_group="*"
                                               ){
                                                 
                      
                                                 
  river <- toString(paste(list("rhine", "elbe", "saar"), collapse = '", "'))                                                                          
                      
  
  body_filter <- paste0(
    
    '{
      "_source": {        
        "excludes": ["eic", "ms1", "ms2"]
      },
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
            },
            {
              "terms": {
                "station": ["',station,'"]
              }
            },
            {
              "terms": {
                "river": ["',river,'"]
              }
            },
            {
              "terms": {
                "matrix": ["',matrix,'"]
              }
            },
            {
              "terms": {
                "tag": ["',tag,'"]
              }
            }
          ],
          "should":[
            {
              "terms": {
                "comp_group": ["*"]
              }
            },
            {
              "terms": {
                "rtt.method": ["*"]
              }
            },
            {
              "terms": {
                "name": ["*"]
              }
            },
            {
              "terms": {
                "ufid": ["*"]
              }
            },
            {
              "range": {
                "rtt.rt": {
                  "gte": 2,
                  "lte": 19
                }
              }
            },
            {
              "range": {
                "mz": {
                  "gte": 145,
                  "lte": 900
                }
              }
            }
          ]
        }
      }
    }'
  )
  return(body_filter)

}



















#xxx <- x_es_fun_query_filter_func_elastic()
start.time <- Sys.time()
test_table <- Search(conn, index = index_list, body = xxx, size = 10000, asdf = TRUE)
test_table <- as.data.table(test_table$hits$hits) 
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken
# 
# bodyx <- '{
# 
#   "query": {
# 
#     "term": {
# 
#       "name": {
# 
#         "value": "Carbamazepine"
# 
#       }
# 
#     }
# 
#   },
# 
#   "size": 1,
# 
#   "_source": ["name", "cas"]
# 
# }'