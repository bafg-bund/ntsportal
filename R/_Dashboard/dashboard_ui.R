dashboard_ui <- function(id, df){
  
  #vars_name <- c("all", unique(test_data$X_source.name))
  #vars_source <- c("all", unique(test_data$X_source.data_source))
  
  navbarPage("my map", id="nav",
             
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
                                        selectInput(NS(id, "filter_station"), "Stations", unique(dashboard_data$location), multiple=TRUE),
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
                        column(3,
                               selectInput(NS(id, "rivers"), "River", unique(dashboard_data$River), multiple=TRUE) 
                        ),
                        column(3,
                               selectInput(NS(id, "stations"), "Station", unique(dashboard_data$Stations), multiple=TRUE)
                        ),
                        column(2,
                               selectInput(NS(id, "ufids"), "Ufid", unique(dashboard_data$Ufid), multiple=TRUE)
                        ),
                        column(2,
                               # numericInput(NS(id, "min_intensity_score"), "Min intensity score", 
                               #              min=min(test_data$X_source.intensity_normalized), 
                               #              max=max(test_data$X_source.intensity_normalized), 
                               #              value=min(test_data$X_source.intensity_normalized),
                               #              step = 0.0001)
                        ),
                        column(2,
                               # numericInput(NS(id, "max_intensity_score"), "Max intensity score", 
                               #              min=min(test_data$X_source.intensity_normalized), 
                               #              max=max(test_data$X_source.intensity_normalized), 
                               #              value=max(test_data$X_source.intensity_normalized),
                               #              step = 0.0001)
                        )
                      ),
                      hr(),
                      DT::dataTableOutput(NS(id, "bafg_data_explorer"))
                      
                      )
  )
}
