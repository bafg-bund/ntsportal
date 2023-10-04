request_ui <- function(){
  fluidPage(
  titlePanel("Title my awesome title"),
  fluidRow(
    column(3, wellPanel(
      h4("pre-request for data param"),
      textInput("control_label",
                "Please enter index, source and the period:",
                "INDEX"),
      
      textInput("control_label_1",
                "Source:",
                "SOURCE"),
      
      dateRangeInput("inDateRange", "Date range input:"),
      
      tags$h5("action Button"),
      actionButton("pre_request", "Action Button", class = "btn-primary")
      
    #  sliderInput("control_num",
    #              "This controls values:",
    #              min = 1, max = 20, value = 15)
    )),
    
    column(3, wellPanel(
      textInput("inText",  "Text input:", value = "station"),
      textInput("inText",  "Text input:", value = "river"),
      textInput("inText",  "Text input:", value = "matrix"),
      textInput("inText",  "Text input:", value = "tag"),
      textInput("inText",  "Text input:", value = "comp group"),
      textInput("inText",  "Text input:", value = "rtt method"),
      
      numericInput("inNumber", "mz input:",
                   min = 1, max = 20, value = 5, step = 0.0005),
      numericInput("inNumber2", "mz toleranz input 2:",
                   min = 0, max = 1, value = 0.5, step = 0.0005),
      
      
      numericInput("inNumber3", "rtt input:",
                   min = 1, max = 20, value = 5, step = 0.0005),
      numericInput("inNumber4", "rtt toleranz input 2:",
                   min = 0, max = 1, value = 0.5, step = 0.0005), 
      
      
#      sliderInput("inSlider", "Slider input:",
#                  min = 1, max = 20, value = 15),
#      sliderInput("inSlider2", "Slider input 2:",
#                  min = 1, max = 20, value = c(5, 15)),
#      sliderInput("inSlider3", "Slider input 3:",
#                  min = 1, max = 20, value = c(5, 15)),
      
#      dateInput("inDate", "Date input:"),
      
#      dateRangeInput("inDateRange", "Date range input:")
    )),
    
    column(3,
           wellPanel(
#             checkboxInput("inCheckbox", "Checkbox input",
#                           value = FALSE),
             textInput("inText",  "Text input:", value = "name"),
             checkboxGroupInput("inCheckboxGroup",
                                "Substance name / id:",
                                c("name" = "option1",
                                  "id" = "option2")),
             radioButtons("inRadio", 
                         "Substance name / id:",
                         c("name" = "option1",
                           "id" = "option2")),
             
             
             pickerInput(inputId = 'pick_ufid', 
                         label = 'ufid:',
                         choices = colnames(data()),
                         options = list(`actions-box` = TRUE), 
                         multiple = T),
             
             
             sliderInput("inSlider2", "mz:",
                         min = 1, max = 20, value = c(5, 15), step = 0.0005), 
             sliderInput("inSlider3", "rtt:",
                         min = 1, max = 20, value = c(5, 15), step = 0.0005),
             
             
             tags$h5("action Button"),
             actionButton("request_filtered_data", "Action Button", class = "btn-primary")
             
     
             
             
#             radioButtons("inRadio", "Radio buttons:",
#                          c("label 1" = "option1",
#                            "label 2" = "option2")),
             

#             selectInput("inSelect2", "Select input 2:",
#                         multiple = TRUE,
#                         c("label 1" = "option1",
#                           "label 2" = "option2"))
           ),
           
#           tabsetPanel(id = "inTabset",
#                       tabPanel("panel1", h2("This is the first panel.")),
#                       tabPanel("panel2", h2("This is the second panel."))
#           )
    )
  )
)
}