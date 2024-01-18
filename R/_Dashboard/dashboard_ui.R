dashboard_ui <- function(id, es_glob_df){
  

  #dashboard_data <- func_get_demo_data_dash
  dashboard_data <- x_func_preprocessing_dashboard_data(data = es_glob_df)
  observeEvent(dashboard_data,{
    print("dash ui update :)")
    #print(dashboard_data)
  })
  
  navbarPage("my map", 
             id=NS(id, "nav"),
             
             tabPanel("Interactive map",
                      div(class="outer",
                          
                          tags$head(
                            # Include our custom CSS
                            includeCSS("R/_Dashboard/styles.css"),
                            includeScript("R/_Dashboard/gomap.js")
                          ),
                          
                          # If not using custom CSS, set height of leafletOutput to a number instead of percent
                          leafletOutput(NS(id, "map"), width="100%", height="100%"),
                          
                          # Shiny versions prior to 0.11 should use class = "modal" instead.
                          absolutePanel(id = "controls", class = "panel panel-default", fixed = TRUE,
                                        draggable = TRUE, top = 130, left = "auto", right = 20, bottom = "auto",
                                        width = 330, height = "auto",
                                        
                                        h2("data explorer"),
                                        
                                        selectInput(NS(id, "filter_ufid"), "Ufid", unique(dashboard_data$Ufid), multiple=TRUE),
                                        selectInput(NS(id, "filter_river"), "River", unique(dashboard_data$River), multiple=TRUE),
                                        selectInput(NS(id, "filter_station"), "Stations", unique(dashboard_data$Stations), multiple=TRUE),
                                        plotOutput(NS(id, "line_plot_intensity"), height = 250),
                                        plotOutput(NS(id, "line_plot_intensity_is"), height = 250)
                          )
                  
                      )
             ),
             
             tabPanel("Data explorer",
                      fluidRow(
                        column(7, plotOutput(NS(id, "line_plot_intensity_is_2"), height = 350)),
                        column(5, leafletOutput(NS(id, "map2"), height = 350))
                      ),
                      hr(),
                      fluidRow(
                        column(2,
                               selectInput(NS(id, "names"), "Name", unique(unlist(dashboard_data$Name)), multiple=TRUE) 
                        ),
                        column(2,
                               selectInput(NS(id, "formulas"), "Formula", unique(unlist(dashboard_data$Formula)), multiple=TRUE)
                        ),
                        column(2,
                               selectInput(NS(id, "cass"), "CAS-RN", unique(unlist(dashboard_data$CAS_RN)), multiple=TRUE)
                        ),
                        column(2,
                               selectInput(NS(id, "methods"), "Chrom. Meth.", unique(dashboard_data$Method), multiple=TRUE)
                        ),
                        column(2,
                               selectInput(NS(id, "matrixs"), "Matrix", unique(dashboard_data$Matrix), multiple=TRUE)
                        ),
                      ),
                      fluidRow(
                        column(6,
                               sliderInput(NS(id, "areas"), "Area:",
                                           min = min(dashboard_data$Area), 
                                           max = max(dashboard_data$Area), 
                                           value = c(min(dashboard_data$Area), 
                                                     max(dashboard_data$Area)), 
                                           step = 0.0001), 
                               ),
                        column(6,
                               sliderInput(NS(id, "intensitys"), "Intensity:",
                                           min = min(dashboard_data$Intensity),
                                           max = max(dashboard_data$Intensity),
                                           value = c(min(dashboard_data$Intensity),
                                                     max(dashboard_data$Intensity)),
                                           step = 0.0001),
                        )
                      ),
                      hr(),
                      DT::dataTableOutput(NS(id, "bafg_data_explorer"))
                      
                      )
  )
}
