request_server <- function(id, df, func_get_index, func_get_parameters){ 
  moduleServer(id, function(input, output, session) {
    
    get_index <- eventReactive(input$get_index,{
      func_get_index
      })
    observe({
      print(get_index()$index[[1]])
      updateSelectInput(session, "in_req_index", choices = get_index()$index[[1]])
      updateSelectInput(session, "in_req_source", choices = get_index()$source[[1]])
      updateDateRangeInput(session, "in_req_date_range",
                           #label = paste("Date range", index),
                           start = today()-365,
                           end = today(),
                           min = today()-7305,
                           max = today()
                           )
      })

    
    get_parameters <- eventReactive(input$get_parameters,{
      func_get_parameters
      }) 
    observe({
      print(get_parameters()$river[[1]])

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
                     input$in_req_date_range[2]))
      
    })
    
    
    get_json_query <- eventReactive(input$request_filtered_data,{
      json_text <- paste0(
      '{
        "query": {
          "range": {
            "timestamp": {
              "gte": "',as.character(input$in_req_date_range[1]),'",
              "lte": "',as.character(input$in_req_date_range[2]),'"
            }
          },
          "filtered" : {
            "query" : {
            	"match_all" : {}
            },
            "filter" : {
            	"term" : {"station" : "',input$in_req_station,'"}
            }
          }
        }
      }'
                          )
      return(json_text)
    })
    # observe({
    #   print(get_json_query)
    # })

    output$json_output <- renderText({
      
      input$request_filtered_data
      req(input$request_filtered_data)
      isolate((jsonlite::prettify(get_json_query(),1)))
    })
    
    })
}
