dashboard_server <- function(id, df){ 
  moduleServer(id, function(input, output, session) {
    
    glob_dashboard_data <- df
    #test_data <- df
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
        return(glob_dashboard_data[FALSE,])
      bounds <- input$map_bounds
      latRng <- range(bounds$north, bounds$south)
      lngRng <- range(bounds$east, bounds$west)
      
      glob_dashboard_data %>% 
        filter(
          is.null(input$filter_river) | glob_dashboard_data$River %in% input$filter_river,
          is.null(input$filter_station) | glob_dashboard_data$location %in% input$filter_station,
          is.null(input$filter_ufid) | glob_dashboard_data$Ufid %in% input$filter_ufid,
          
        ) %>%
        subset(lat >= latRng[1] & lat <= latRng[2] & lon >= lngRng[1] & lon <= lngRng[2])


    })

    
    
    output$line_plot_intensity <- renderPlot({
      # If no stations are in view, don't plot
      if (nrow(station_bounds()) == 0)
        return(NULL)
      
      ggplot(station_bounds()) +
        geom_line(aes(x = as.POSIXct(Time), y = Intensity, color = River), alpha = 0.5) +
        scale_colour_discrete(name  ="River") +
        xlab(label = NULL) +
        ylab(label = "Intensity")
    })
    
    output$line_plot_intensity_is <- renderPlot({
      # If no stations are in view, don't plot
      if (nrow(station_bounds()) == 0)
        return(NULL)
      
      ggplot(station_bounds()) +
        geom_line(aes(x = as.POSIXct(Time), y = Area, color = River), alpha = 0.5) +
        scale_colour_discrete(name  ="River") +
        xlab(label = NULL) +
        ylab(label = "Area")
    })
    
    
    # This observer is responsible for maintaining the circles and legend,
    # according to the variables the user has chosen to map to color and size.
    observe({
      
      
      summ_data <- station_bounds() %>%
        group_by(location) %>%
        reframe(Detections=n(),
                  lon=unique(lon),
                  lat=unique(lat),
                  Name=toString(na.omit(unique(Name))),
                  Formula=toString(na.omit(unique(Formula))),
                  Ufid=toString(na.omit(unique(Ufid))),
                  tRet=mean(tRet),
                  mz=mean(mz),
                  Classification=toString(na.omit(unique(Classification))),
                  Area=median(Area),
                  Area_normalized=median(Area_normalized),
                  Stations=toString(na.omit(unique(Stations))),
                  River=toString(na.omit(unique(River))),
                  Intensity=mean(Intensity)
        )

      

        colorData <- glob_dashboard_data[["Intensity"]]
        pal <- colorBin("viridis", colorData, 7, pretty = FALSE)
        

        leafletProxy("map", data = summ_data) %>%
        clearShapes() %>%
        addCircleMarkers(~lon, ~lat,
                   radius=6,
                   layerId=~River, #X_source.station,
                   stroke=FALSE,
                   fillOpacity=0.7,
                   fillColor=~pal(Intensity)
                   ) %>%
        addLegend("bottomright", pal=pal, values=colorData, title="Intensity", layerId="colorLegend")
    })
    
    # Show a popup at the given location
    show_staion_popup <- function(River, lat, lng) {
      selected_station <- summ_data[summ_data$River == River,]
      content <- as.character(tagList(
        tags$h4("Station:", selected_station$Stations),
        tags$strong(HTML(sprintf("%s, %s %s",
                                 unique(selected_station$River), 
                                 unique(selected_station$Classification), 
                                 unique(selected_station$Formula)
        ))), tags$br(),
        sprintf("Median rt: %s", as.numeric(median(selected_station$tRet ))), tags$br(),
        sprintf("Median mz: %s", as.numeric(median(selected_station$mz))), tags$br(),
        sprintf("Median intensity: %s", as.numeric(median(selected_station$Intensity)))
      ))
      leafletProxy("map") %>% addPopups(lng, lat, content, layerId = River)
    }

    # When map is clicked, show a popup with info
    observe({
      leafletProxy("map") %>% clearPopups()
      event <- input$map_marker_click
      if (is.null(event))
        return()

      isolate({
        show_staion_popup(event$id, event$lat, event$lng)
      })
    })
    
    
    
    ## Data Explorer ###########################################
    
    
    reactive_bafg_data_explorer <- reactive({
      
      glob_dashboard_data %>% 
        filter(
          #test_data$X_source.intensity_normalized >= as.numeric(input$min_intensity_score),
          #test_data$X_source.intensity_normalized <= as.numeric(input$max_intensity_score),
          is.null(input$rivers) | glob_dashboard_data$River %in% input$rivers,
          is.null(input$stations) | glob_dashboard_data$Stations %in% input$stations,
          is.null(input$ufids) | glob_dashboard_data$Ufid %in% input$ufids
          )
      
    })
    
    shared_bafg_data_explorer <- SharedData$new(reactive_bafg_data_explorer)
    #reactive(shared_bafg_data_explorer$data(withSelection = TRUE))
    
    
    df_ggplot <- debounce(reactive_bafg_data_explorer, millis = 250)
    output$line_plot_intensity_is_2 <- renderPlot({
      ggplot(df_ggplot()) +
        geom_line(aes(x = as.POSIXct(Time), y = Intensity, color = River), alpha = 0.5) +
        scale_colour_discrete(name  ="River") +
        xlab(label = NULL) +
        ylab(label = "Intensity")
    })
    
    
    output$map2 <- renderLeaflet({
      
      summ_data_exp <- reactive_bafg_data_explorer() %>%
        group_by(location) %>%
        reframe(Detections=n(),
                lon=unique(lon),
                lat=unique(lat),
                Name=toString(na.omit(unique(Name))),
                Formula=toString(na.omit(unique(Formula))),
                Ufid=toString(na.omit(unique(Ufid))),
                tRet=mean(tRet),
                mz=mean(mz),
                Classification=toString(na.omit(unique(Classification))),
                Area=median(Area),
                Area_normalized=median(Area_normalized),
                Stations=toString(na.omit(unique(Stations))),
                River=toString(na.omit(unique(River))),
                Intensity=mean(Intensity)
        )
      

      colorData <- shared_bafg_data_explorer[["X_source.intensity"]]
      pal <- colorBin("viridis", colorData, 7, pretty = FALSE)
      
      leaflet(summ_data_exp) %>%
        addTiles() %>%
        setView(lat = 50.04, lng = 8.25, zoom = 8) %>% # zoom orig 6 !!!
        addMarkers(~lon,
                   ~lat)
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