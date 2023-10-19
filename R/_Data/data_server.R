data_server <- function(id, func_get_data_data){ 
  moduleServer(id, function(input, output, session) {
    
     data_explorer_data <- func_get_data_data
    # 
    # observe({ 
    #   print(colnames(data_explorer_data))
    #   updatePickerInput(session, "picker", choices = colnames(data_explorer_data))
    # })

    
    datasetInput <- eventReactive(input$view_bafg_data,{
      datasetInput <- data_explorer_data %>%
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
