dashboard_server <- function(id, func_get_dashboard_data){ 
  moduleServer(id, function(input, output, session) {
    
    
    
    glob_dashboard_data <- func_get_dashboard_data
    
    output$map <- renderLeaflet({
      leaflet() %>%
        addTiles() %>%
        setView(lat = 50.34, lng = 7.59, zoom = 6) # zoom orig 6 !!!
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
          is.null(input$filter_station) | glob_dashboard_data$Stations %in% input$filter_station,
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
        group_by(Stations) %>%
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
                   layerId=~Stations, #X_source.station,
                   stroke=FALSE,
                   fillOpacity=0.7,
                   fillColor=~pal(Intensity)
                   ) %>%
        addLegend("bottomright", pal=pal, values=colorData, title="Intensity", layerId="colorLegend")
    })
    
    # Show a popup at the given Stations
    show_staion_popup <- function(Stations, lat, lng, summ_data) {
      selected_station <- summ_data[summ_data$Stations == Stations,]
      content <- as.character(tagList(
        tags$h4("Station:", selected_station$Stations, 
                "Detections:", selected_station$Detections),
        tags$strong(HTML(sprintf("River: %s", unique(selected_station$River) ))),
        tags$br(),
        tags$strong(HTML(sprintf("Classification: %s", na.omit(unique(unlist(selected_station$Classification))) ))),
        tags$br(),
        tags$strong(HTML(sprintf("Formula: %s", na.omit(unique(unlist(selected_station$Formula))) ))),
        tags$br(),
        tags$strong(HTML(sprintf("Name: %s", na.omit(unique(unlist(selected_station$Name))) ))),  #list(na.omit(unique(unlist(temp_data$X_source.name))))
        tags$br(),
        tags$strong(HTML(sprintf("Ufid: %s", na.omit(unique(unlist(selected_station$Ufid))) ))),
        tags$br(),
        sprintf("Median rt: %s", as.numeric(median(selected_station$tRet ))), 
        tags$br(),
        sprintf("Median mz: %s", as.numeric(median(selected_station$mz))), 
        tags$br(),
        sprintf("Mean intensity: %s", as.numeric(median(selected_station$Intensity))),
        tags$br(),
        sprintf("Median Area: %s", as.numeric(median(selected_station$Area)))
      ))
      leafletProxy("map") %>% addPopups(lng, lat, content, layerId = Stations)
    }
    # When map is clicked, show a popup with info
    observe({
      summ_data <- station_bounds() %>%
        group_by(Stations) %>%
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
      
      leafletProxy("map") %>% clearPopups()
      event <- input$map_marker_click
      if (is.null(event))
        return()

      isolate({
        show_staion_popup(event$id, event$lat, event$lng, summ_data)
      })
    })
    
    
    
    ## Data Explorer ###########################################
    
    
    reactive_bafg_data_explorer <- reactive({
      
      glob_dashboard_data %>% 
        filter(
          is.null(input$names) | glob_dashboard_data$Name %in% input$names,
          is.null(input$formulas) | glob_dashboard_data$Formula %in% input$formulas,
          is.null(input$cass) | glob_dashboard_data$CAS_RN %in% input$cass,
          is.null(input$methods) | glob_dashboard_data$Method %in% input$methods,
          is.null(input$matrixs) | glob_dashboard_data$Matrix %in% input$matrixs
          #between(as.numeric(glob_dashboard_data$Area), input$areas[1], input$areas[2])
          # glob_dashboard_data$Area >= input$areas[1],
          # glob_dashboard_data$Area <= input$areas[2]
          ) %>%
        as.data.table() %>%
        subset(Area >= input$areas[1] & Area <= input$areas[2] &
                 Intensity >= input$intensitys[1] & Intensity <= input$intensitys[2])
      
    })
    
    shared_bafg_data_explorer <- SharedData$new(reactive_bafg_data_explorer)
    #reactive(shared_bafg_data_explorer$data(withSelection = TRUE))
    
    
    df_ggplot <- debounce(reactive(shared_bafg_data_explorer$data(withSelection = TRUE)), millis = 250)
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
        setView(lat = 50.34, lng = 7.59, zoom = 6) %>% # zoom orig 6 !!!
        addMarkers(~lon,
                   ~lat)
    })

    
    output$bafg_data_explorer <- DT::renderDataTable({
      target <- which(names(shared_bafg_data_explorer$data()) %in% c("lon", "lat", "location")) - 1
      DT::datatable(shared_bafg_data_explorer,
                    options = list(
                      columnDefs = list(list(visible=FALSE, targets=target)),  #<- y ???
                      searchHighlight = TRUE)
                    #filter = "top"
                    )
    }, server = FALSE)
    

    
  })
}