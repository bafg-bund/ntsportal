data_ui <- function(id, es_glob_df){ 
  
  fluidPage(
    titlePanel("bafg data explorer"),
    fluidRow(
      column(1,
             tags$br(),
             actionButton(NS(id, "view_bafg_data"), "View Selection", class = "btn-primary"),
             tags$br(),tags$br(),tags$br(),
             pickerInput(NS(id, "picker"), "Choose", choices = colnames(es_glob_df), 
                         options = list(`actions-box` = TRUE), multiple = TRUE)
             
             ),
      column(11,
             tags$br(),
               DT::dataTableOutput(NS(id, "bafg_data"))
               
               #)
             )
      )
    )
  }

