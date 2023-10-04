request_ui <- function(id, df){ 
  
  
  fluidPage(
  titlePanel("Title my awesome title"),
  fluidRow(
    column(3, wellPanel(
      h4("pre-request for data param"),
      textInput(NS(id, "in_req_index"),
                "Please enter index, source and the period:",
                "INDEX"),
      
      textInput(NS(id, "in_req_source"),
                "Source:",
                "SOURCE"),
      
      dateRangeInput(NS(id, "inDateRange_req"), "Date range input:"),
      
      tags$h5("action Button"),
      actionButton(NS(id, "pre_request"), "Action Button", class = "btn-primary")
      
      
    )),
    
    column(3, wellPanel(
      textInput(NS(id, "inText_req_station"),  "Station input:", value = "station"),
      textInput(NS(id, "inText_req_river"),  "River input:", value = "river"),
      selectInput(NS(id, "inText_req_matrix"), label = "Matrix input:", choices = NULL, multiple = TRUE),
      #textInput("inText_req_matrix",  "Matrix input:", value = "matrix"),
      textInput(NS(id, "inText_req_tag"),  "Tag input:", value = "tag"),
      textInput(NS(id, "inText_req_comp_group"),  "Comp Group input:", value = "comp group"),
      selectInput(NS(id, "inText_req_rtt_method"), label = "rtt Method input:", choices = NULL, multiple = TRUE),
      #textInput("inText_req_rtt_method",  "rtt Method input:", value = "rtt method"),
      
      numericInput(NS(id, "inNumber_req_mz"), "mz input:",
                   min = 1, max = 20, value = 5, step = 0.0005),
      numericInput(NS(id, "inNumber_req_mz_tol"), "mz toleranz input:",
                   min = 0, max = 1, value = 0.5, step = 0.0005),
      
      
      numericInput(NS(id, "inNumber_req_rtt"), "rtt input:",
                   min = 1, max = 20, value = 5, step = 0.0005),
      numericInput(NS(id, "inNumber_req_rtt_tol"), "rtt toleranz input:",
                   min = 0, max = 1, value = 0.5, step = 0.0005), 
      
      
    )),
    
    column(3,
           wellPanel(
             
             textInput(NS(id, "inText"),  "Text input:", value = "name"),
             checkboxGroupInput(NS(id, "inCheckboxGroup"),
                                "Substance name / id:",
                                c("name" = "option1",
                                  "id" = "option2")),
             radioButtons(NS(id, "inRadio"), 
                          "Substance name / id:",
                          c("name" = "option1",
                            "id" = "option2")),
             
             selectInput(NS(id, "ufid"), label = "ufid:", choices = NULL, multiple = TRUE),
             
             
             #                           pickerInput(inputId = 'pick_ufid', 
             #                                       label = 'ufid:',
             #                                       choices = colnames(data()),
             #                                       options = list(`actions-box` = TRUE), 
             #                                       multiple = T),
             
             
             sliderInput(NS(id, "inSlider_req_mz"), "mz:",
                         min = 1, max = 20, value = c(5, 15), step = 0.0005), 
             sliderInput(NS(id, "inSlider_req_rtt"), "rtt:",
                         min = 1, max = 20, value = c(5, 15), step = 0.0005),
             
             
             tags$h5("action Button"),
             actionButton(NS(id, "request_filtered_data"), "Action Button", class = "btn-primary")
             
             
           ),
           
           #           tabsetPanel(id = "inTabset",
           #                       tabPanel("panel1", h2("This is the first panel.")),
           #                       tabPanel("panel2", h2("This is the second panel."))
           #           )
    )
  )
)
  
  
}

