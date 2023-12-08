data_ui <- function(id, func_get_data_data, es_glob_df){ 
  
  #data_explorer_data_colnames <-colnames(es_glob_df)
  observe({ 
    print("data_ui_col_names")
    data_explorer_data_colnames <- colnames(es_glob_df)
  })
  
  fluidPage(
    titlePanel("bafg data explorer"),
    fluidRow(
      column(1,
             tags$br(),
             actionButton(NS(id, "view_bafg_data"), "View Selection", class = "btn-primary"),
             tags$br(),tags$br(),tags$br(),
             pickerInput(NS(id, "picker"), "Choose", choices = NULL, #data_explorer_data_colnames,
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

