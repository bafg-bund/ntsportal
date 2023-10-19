request_ui <- function(id, df){ 
  
  fluidPage(
  titlePanel("Title my awesome title"),
  fluidRow(
    column(3, wellPanel(
      h4("pre-request for data param"),
      #--------------------------------get index
      tags$h5("get index"),
      actionButton(NS(id, "get_index"), "connect to elastic", class = "btn-primary"),
      
      tags$h5("Please enter index, source and the period:"),
      
      selectInput(NS(id, "in_req_index"), "Index:", choices = NULL, multiple = TRUE),

      selectInput(NS(id, "in_req_source"), "Source:", choices = NULL, multiple = TRUE),
      
      dateRangeInput(NS(id, "in_req_date_range"), "Date range:"),
      
      tags$h5("get parameters"),
      actionButton(NS(id, "get_parameters"), "connect to elastic", class = "btn-primary"),
      tags$br(),tags$br(),tags$br(),tags$br(),tags$br(),tags$br(),
      verbatimTextOutput(NS(id, "text_req_index"))
      )),
    
    column(3, wellPanel(
      selectInput(NS(id, "in_req_station"),  "Station input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_river"),  "River input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_matrix"), "Matrix input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_tag"),  "Tag input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_comp_group"),  "Classification input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_rtt_method"), "Chrom. Meth.input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_name"), "Name input:", 
                  choices = NULL, multiple = TRUE),
      selectInput(NS(id, "in_req_ufid"), "Ufid input:", 
                  choices = NULL, multiple = TRUE)
      )),
    
    column(3,wellPanel(
      numericInput(NS(id, "in_number_req_mz"), "mz input:", 
                   min = 1, max = 20, value = 5, step = 0.0005),
      numericInput(NS(id, "in_number_req_mz_tol"), "mz tolerance input:",
                   min = 0, max = 10, value = 0.5, step = 0.0005),
             
      sliderInput(NS(id, "in_slider_req_mz"), "mz:",
                  min = 1, max = 20, value = c(5, 15), step = 0.0005), 
      
      tags$br(),tags$br(),tags$br(),tags$br(),
             
      numericInput(NS(id, "in_number_req_rtt"), "rt input:",
                   min = 1, max = 20, value = 5, step = 0.0005),
      numericInput(NS(id, "in_number_req_rtt_tol"), "rt tolerance input:",
                   min = 0, max = 10, value = 0.5, step = 0.0005), 
             

      sliderInput(NS(id, "in_slider_req_rtt"), "rt:", 
                  min = 1, max = 20, value = c(5, 15), step = 0.0005),
             
      
      tags$h5("get data"),
      actionButton(NS(id, "request_filtered_data"), "connect to elastic", class = "btn-primary")
      )),
    
    column(3,
           verbatimTextOutput(NS(id, "json_output"))
           )
    )
  )}

