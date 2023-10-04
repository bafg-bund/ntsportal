dashboard_server <- function(id){ 
  moduleServer(id, function(input, output, session) {
    
    
    output$map <- renderLeaflet({
      leaflet() %>%
        addTiles() %>%
        setView(lat = 49.04, lng = 8.25, zoom = 6)
    })
    

    
  })
}