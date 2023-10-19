data_ui <- function(id, func_get_data_data){ 
  
  data_explorer_data_colnames <-colnames(func_get_data_data)
  
  fluidPage(
    titlePanel("bafg data explorer"),
    fluidRow(
      column(1,
             tags$br(),
             actionButton(NS(id, "view_bafg_data"), "View Selection", class = "btn-primary"),
             tags$br(),tags$br(),tags$br(),
             pickerInput(NS(id, "picker"), "Choose", choices = data_explorer_data_colnames,
                         options = list(`actions-box` = TRUE), multiple = TRUE)
             
             ),
      column(11,
             tags$br(),
             #box(
               #width = NULL,
               #status = "primary",
               #title = "bafg data",
               DT::dataTableOutput(NS(id, "bafg_data"))
               
               #)
             )
      )
    )
  }

