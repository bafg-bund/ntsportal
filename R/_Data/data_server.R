data_server <- function(id, es_glob_df){ 
  moduleServer(id, function(input, output, session) {
    
    datasetInput <- eventReactive(input$view_bafg_data,{
      # print(is.reactive( es_glob_df() )) # for debugging
      datasetInput <- es_glob_df() %>%  
        select(input$picker)
      return(datasetInput)
    })
    
    output$bafg_data <- DT::renderDataTable({
      DT::datatable(datasetInput(),
                    extensions = 'Buttons',
                    
                    options = list(
                      paging = TRUE,
                      searching = TRUE,
                      fixedColumns = TRUE,
                      autoWidth = TRUE,
                      ordering = TRUE,
                      dom = 'Bfrtip',
                      buttons = c('csv', 'excel'), #'copy', 
                      searchHighlight = TRUE,
                      autoFill = TRUE
                    ),
                    
                    class = "display",
                    

                    
                    # options = list(searchHighlight = TRUE,
                    #                buttons = list("copy", "csv")),
                    filter = "top"
      )
    }, server = FALSE) #, server = FALSE
    
  })
}
