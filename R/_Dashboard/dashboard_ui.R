dashboard_ui <- function(id){
  
  # !!!!!!!!! NS ID !!!!!!!
  # Choices for drop-downs
  vars <- c(
    "Is SuperZIP?" = "superzip",
    "Centile score" = "centile",
    "College education" = "college",
    "Median income" = "income",
    "Population" = "adultpop"
  )
  
  
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
                                        
                                        selectInput("color", "Color", vars),
                                        selectInput("size", "Size", vars, selected = "adultpop"),
                                        conditionalPanel("input.color == 'superzip' || input.size == 'superzip'",
                                                         # Only prompt for threshold when coloring or sizing by superzip
                                                         numericInput("threshold", "SuperZIP threshold (top n percentile)", 5)
                                        ),
                                        
                                        plotOutput("histCentile", height = 200),
                                        plotOutput("scatterCollegeIncome", height = 250)
                          )
                  
                      )
             ),
             
             tabPanel("Data explorer")
  )
  
  
}
