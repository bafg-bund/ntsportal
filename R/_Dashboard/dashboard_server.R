dashboard_server <- function(id, df){ 
  moduleServer(id, function(input, output, session) {
    
    test_data <- df
    #test_data <- dashboard_data
    
    output$map <- renderLeaflet({
      leaflet() %>%
        addTiles() %>%
        setView(lat = 49.04, lng = 8.25, zoom = 8) # zoom orig 6 !!!
    })
    
    
   # A reactive expression that returns the set of stations that are
   # in bounds right now
    station_bounds <- reactive({
      if (is.null(input$map_bounds))
        return(test_data[FALSE,])
      bounds <- input$map_bounds
      latRng <- range(bounds$north, bounds$south)
      lngRng <- range(bounds$east, bounds$west)

      subset(test_data,
             X_source.loc.lat >= latRng[1] & X_source.loc.lat <= latRng[2] &
               X_source.loc.lon >= lngRng[1] & X_source.loc.lon <= lngRng[2])
    })

    
    
    output$line_plot_intensity <- renderPlot({
      # If no stations are in view, don't plot
      #if (nrow(station_bounds()) == 0)
      #  return(NULL)
      
      ggplot(station_bounds()) +
        geom_line(aes(x = as.POSIXct(X_source.start), y = X_source.intensity, color = X_source.river), alpha = 0.5) +
        scale_colour_discrete(name  ="River") +
        xlab(label = NULL) +
        ylab(label = "Intensity")
    })
    
    output$line_plot_intensity_is <- renderPlot({
      # If no stations are in view, don't plot
      #if (nrow(station_bounds()) == 0)
      #  return(NULL)
      
      ggplot(station_bounds()) +
        geom_line(aes(x = as.POSIXct(X_source.start), y = X_source.intensity_is, color = X_source.river), alpha = 0.5) +
        scale_colour_discrete(name  ="River") +
        xlab(label = NULL) +
        ylab(label = "Intensity (is)")
    })
    
    
    # This observer is responsible for maintaining the circles and legend,
    # according to the variables the user has chosen to map to color and size.
    observe({
      #colorBy <- input$color #test_data$X_source.station
      #sizeBy <- input$size #test_data$X_source.intensity
      
#      if (colorBy == "superzip1") {
#        # Color and palette are treated specially in the "superzip" case, because
#        # the values are categorical instead of continuous.
#        colorData <- ifelse(test_data$X_source.intensity >= (100 - 5), "yes", "no")
#        pal <- colorFactor("viridis", colorData)
#      } else {
        #colorData <- ifelse(test_data$X_source.intensity >= (100 - 5), "yes", "no")
        colorData <- test_data[["X_source.intensity"]]
        pal <- colorBin("viridis", colorData, 7, pretty = FALSE)
#      }

#      if (sizeBy == "superzip") {
#        # Radius is treated specially in the "superzip" case.
#        radius <- ifelse(test_data$X_source.intensity >= (100 - 5), 30000, 3000)
#      } else {
        #radius <- zipdata[[sizeBy]] / max(zipdata[[sizeBy]]) * 30000
        radius <- test_data[["X_source.intensity"]] / max(test_data[["X_source.intensity"]]) * 10000
#      }
        
        #test_data$st <- ifelse(test_data$X_source.loc.lon >= 7.5992, "st1", "st2")
      
        leafletProxy("map", data = station_bounds()) %>%
        clearShapes() %>%
        addCircles(~X_source.loc.lon, 
                   ~X_source.loc.lat, 
                   radius=radius, 
                   layerId=~X_source.river, #X_source.station,
                   stroke=FALSE, 
                   fillOpacity=0.4,
                   fillColor=pal(colorData)
                   ) %>%
        addLegend("bottomright", pal=pal, values=colorData, title="Intensity", layerId="colorLegend")
    })
    
    # Show a popup at the given location
    show_staion_popup <- function(X_source.river, lat, lng) {
      selected_station <- test_data[test_data$X_source.river == X_source.river,]#allzips[allzips$zipcode == zipcode,] #selectedZip
      content <- as.character(tagList(
        tags$h4("Score:", as.numeric(median(selected_station$X_source.intensity))),
        tags$strong(HTML(sprintf("%s, %s %s",
                                 unique(selected_station$X_source.river), 
                                 unique(selected_station$X_source.name_is), 
                                 unique(selected_station$X_source.station)
        ))), tags$br(),
        sprintf("Median rt: %s", as.numeric(median(selected_station$X_source.rt ))), tags$br(),
        sprintf("Median mz: %s", as.numeric(median(selected_station$X_source.mz))), tags$br(),
        sprintf("Median intensity: %s", as.numeric(median(selected_station$X_source.intensity)))
      ))
      leafletProxy("map") %>% addPopups(lng, lat, content, layerId = X_source.river)
    }

    # When map is clicked, show a popup with info
    observe({
      leafletProxy("map") %>% clearPopups()
      event <- input$map_shape_click
      if (is.null(event))
        return()

      isolate({
        show_staion_popup(event$id, event$lat, event$lng)
      })
    })
    
    
    
    ## Data Explorer ###########################################
    
    
    reactive_bafg_data_explorer <- reactive({
      
      test_data %>% 
        filter(
          #test_data$X_source.intensity_normalized >= as.numeric(input$min_intensity_score),
          #test_data$X_source.intensity_normalized <= as.numeric(input$max_intensity_score),
          is.null(input$rivers) | test_data$X_source.river %in% input$rivers,
          is.null(input$stations) | test_data$X_source.station %in% input$stations,
          is.null(input$sources) | test_data$X_source.data_source %in% input$sources
          ) %>%
        select(!c("X_source.ms2","X_source.eic", "X_source.rtt"))
      
    })
    
    shared_bafg_data_explorer <- SharedData$new(reactive_bafg_data_explorer)
    
    df_ggplot <- debounce(reactive(shared_bafg_data_explorer$data(withSelection = TRUE)), millis = 250)
    output$line_plot_intensity_is_2 <- renderPlot({
      ggplot(df_ggplot()) +
        geom_line(aes(x = as.POSIXct(X_source.start), y = X_source.intensity_is, color = X_source.river), alpha = 0.5) +
        scale_colour_discrete(name  ="River") +
        xlab(label = NULL) +
        ylab(label = "Intensity (is)")
    })
    
    
    output$map2 <- renderLeaflet({
      

      colorData <- shared_bafg_data_explorer[["X_source.intensity"]]
      pal <- colorBin("viridis", colorData, 7, pretty = FALSE)
      
      leaflet(shared_bafg_data_explorer) %>%
        addTiles() %>%
        setView(lat = 50.04, lng = 8.25, zoom = 8) %>% # zoom orig 6 !!!
        addMarkers(~X_source.loc.lon,
                   ~X_source.loc.lat)
        # addCircles(~X_source.loc.lon,
        #            ~X_source.loc.lat,
        #            radius=shared_bafg_data_explorer[["X_source.intensity"]] / max(shared_bafg_data_explorer[["X_source.intensity"]]) * 10000,
        #            layerId=~X_source.river, #X_source.station,
        #            stroke=FALSE,
        #            fillOpacity=0.4,
        #            fillColor=pal(colorData)
        # )
    })

    
    output$bafg_data_explorer <- DT::renderDataTable({
      DT::datatable(shared_bafg_data_explorer)
    }, server = FALSE)
    

    
  })
}