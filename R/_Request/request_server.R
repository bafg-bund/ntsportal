request_server <- function(id, df, x_es_fun_get_data_from_elastic_1, func_get_parameters_1){ #func_get_index, x_es_fun_list_indices,
  moduleServer(id, function(input, output, session) {
    
    # index_source_list <- reactiveVal()
    # observeEvent(input$get_index,{
    #   index_source_list <- x_es_fun_list_indices
    # })
    
    get_index <- eventReactive(input$get_index,{
      x_es_fun_list_indices()
      })
    observe({
      print(get_index())#$index[[1]])
      updateSelectInput(session, "in_req_index", choices = get_index()$index[[1]], selected = get_index()$index[[1]])
      #updateSelectInput(session, "in_req_source", choices = get_index()$source[[1]])
      updateSelectInput(session, "in_req_source", choices = get_index()$source[[1]], selected = get_index()$source[[1]])
      updateDateRangeInput(session, "in_req_date_range",
                           #label = paste("Date range", index),
                           start = today()-365*10-2,
                           end = today(),
                           min = today()-7305,
                           max = today()
                           )
      })

    
    
    
    
    
    get_parameters <- eventReactive(input$get_parameters,{
      print(input$in_req_date_range) # for debugging
      x_es_func_get_parameters(index_list = input$in_req_index, data_source = input$in_req_source, date_start = input$in_req_date_range[1], date_end = input$in_req_date_range[2], size = 10000)
      }) 
    observe({
      print(get_parameters()$comp_group[[1]]) # for debugging
      

      updateSelectInput(session, "in_req_station", choices = get_parameters()$station[[1]])
      updateSelectInput(session, "in_req_river", choices = get_parameters()$river[[1]])
      updateSelectInput(session, "in_req_matrix", choices = get_parameters()$matrix[[1]])
      updateSelectInput(session, "in_req_tag", choices = get_parameters()$tag[[1]])
      updateSelectInput(session, "in_req_comp_group", choices = get_parameters()$comp_group[[1]])
      updateSelectInput(session, "in_req_rtt_method", choices = get_parameters()$rtt_method[[1]])
      updateSelectInput(session, "in_req_name", choices = get_parameters()$name[[1]])
      updateSelectInput(session, "in_req_ufid", choices = get_parameters()$ufid[[1]])

      updateSliderInput(session, "in_slider_req_mz",
                        min = min(get_parameters()$mz_min_max),
                        max = max(get_parameters()$mz_min_max),
                        value = c(min(get_parameters()$mz_min_max), 
                                  max(get_parameters()$mz_min_max)
                                  ))
      updateNumericInput(session, "in_number_req_mz",
                         value = mean(get_parameters()$mz_min_max), 
                         min = min(get_parameters()$mz_min_max), 
                         max = max(get_parameters()$mz_min_max), 
                         step = 0.0005)
      
      updateSliderInput(session, "in_slider_req_rtt",
                        min = min(get_parameters()$rt_min_max),
                        max = max(get_parameters()$rt_min_max),
                        value = c(min(get_parameters()$rt_min_max), 
                                  max(get_parameters()$rt_min_max)
                                  )) 
      updateNumericInput(session, "in_number_req_rtt",
                         value = mean(get_parameters()$rt_min_max), 
                         min = min(get_parameters()$rt_min_max), 
                         max = max(get_parameters()$rt_min_max), 
                         step = 0.0005)
      })
    
    output$text_req_index <- renderText({
      input$get_parameters
      req(input$get_parameters)
      isolate(paste0("your indeces: ", input$in_req_index, "\n",
                     "your sources: ", input$in_req_source, "\n",
                     "your date range:\n\t from: ", input$in_req_date_range[1], 
                     "\n\t to: ", 
                     input$in_req_date_range[2],"\n\n"))
      
    })
    
    
    get_json_query <- eventReactive(input$request_filtered_data,{
      # json_text <- paste0(
      # '{
      #   "query": {
      #     "range": {
      #       "timestamp": {
      #         "gte": "',as.character(input$in_req_date_range[1]),'",
      #         "lte": "',as.character(input$in_req_date_range[2]),'"
      #       }
      #     },
      #     "filtered" : {
      #       "query" : {
      #       	"match_all" : {}
      #       },
      #       "filter" : {
      #       	"term" : {"station" : "',input$in_req_station,'"}
      #       }
      #     }
      #   }
      # }'
      #                     )
      # return(json_text)
      
         data <- x_es_fun_get_data_from_elastic(index_list = input$in_req_index)#(index_list = input$in_req_index)
         #print(data)
         return(data)
      
      
    })
    
    
    
    
    
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
    
    es_globe_df <- reactive({
      es_globe_df <- get_json_query()
      return(es_globe_df)
    })
    
    observe({
      print(get_json_query())
      es_glob_df <<- get_json_query()
    })

    #output$json_output <- renderText({
      
      #input$request_filtered_data
      #req(input$request_filtered_data)
      
      #isolate((jsonlite::prettify(get_json_query(),1)))
    #})
    
     return( reactive({es_glob_df}))
     
    })
}
