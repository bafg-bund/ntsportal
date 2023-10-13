leaflet(data = test_data) %>%
  addTiles() %>%
  setView(lat = 49.04, lng = 8.25, zoom = 8) %>%
  #leafletProxy("map", data = test_data) %>%
  clearShapes() %>%
  addCircles(~X_source.loc.lon, 
             ~X_source.loc.lat, 
             radius=radius, 
             layerId=~st,
             stroke=FALSE, 
             fillOpacity=0.4, 
             #color = "blue",
             fillColor=pal(colorData)
  )  %>%
  addLegend("bottomleft", pal=pal, values=colorData, title=colorBy, layerId="colorLegend")




content <- as.character(tagList(
  tags$h4("Score:", as.numeric(median(test_data$X_source.intensity))),
  tags$strong(HTML(sprintf("%s, %s %s",
                           unique(test_data$X_source.river), 
                           unique(test_data$X_source.name_is), 
                           unique(test_data$X_source.station)
  ))), 
  tags$br(),
  sprintf("Median rt: %s", as.numeric(median(test_data$X_source.rt ))), 
  tags$br(),
  sprintf("Median mz: %s%%", as.numeric(median(test_data$X_source.mz))), tags$br(),
  sprintf("Median intensity: %s", as.numeric(median(test_data$X_source.intensity)))
))



ggplot(test_data) +
  geom_line(aes(x = as.POSIXct(X_source.start), y = X_source.intensity_is, color = X_source.river), alpha = 0.5) +
  scale_colour_discrete(name  ="River") +
  xlab(label = NULL) +
  ylab(label = "Intensity_is")
  
  #theme(legend.title = element_text( "River"))

test_data[,-c("X_source.ms2","X_source.eic", "X_source.rtt")]



output$map2 <- renderLeaflet({

  df_filter_2 <- test_data %>%
    filter(
      X_source.intensity_normalized >= input$min_intensity_score,
      X_source.intensity_normalized <= input$max_intensity_score,
      is.null(input$rivers) | X_source.river %in% input$rivers,
      is.null(input$stations) | X_source.station %in% input$stations,
      is.null(input$sources) | X_source.data_source %in% input$sources
    ) %>%
    select(!c("X_source.ms2","X_source.eic", "X_source.rtt"))

  colorData <- df_filter_2[["X_source.intensity"]]
  pal <- colorBin("viridis", colorData, 7, pretty = FALSE)

  leaflet(df_filter_2) %>%
    addTiles() %>%
    setView(lat = 49.04, lng = 8.25, zoom = 8) %>% # zoom orig 6 !!!
    addCircles(~X_source.loc.lon,
               ~X_source.loc.lat,
               radius=df_filter_2[["X_source.intensity"]] / max(df_filter_2[["X_source.intensity"]]) * 10000,
               layerId=~X_source.river, #X_source.station,
               stroke=FALSE,
               fillOpacity=0.4,
               fillColor=pal(colorData)
    )
})





# observe({
#   stations <- if (is.null(input$rivers)) character(0) else {
#     filter(test_data, X_source.river %in% input$rivers) %>%
#       `$`('X_source.station') %>%
#       unique() %>%
#       sort()
#   }
#   stillSelected <- isolate(input$stations[input$stations %in% stations])
#   updateSelectizeInput(session, "stations", choices = stations,
#                        selected = stillSelected, server = TRUE)
# })
# 
# observe({
#   sources <- if (is.null(input$rivers)) character(0) else {
#     test_data %>%
#       filter(X_source.river %in% input$rivers,
#              is.null(input$station) | X_source.station %in% input$stations) %>%
#       `$`('X_source.data_source') %>%
#       unique() %>%
#       sort()
#   }
#   stillSelected <- isolate(input$sources[input$sources %in% sources])
#   updateSelectizeInput(session, "sources", choices = sources,
#                        selected = stillSelected, server = TRUE)
# })





# observe({
#   if (is.null(input$goto))
#     return()
#   isolate({
#     map <- leafletProxy("map")
#     map %>% clearPopups()
#     dist <- 0.5
#     zip <- input$goto$zip
#     lat <- input$goto$lat
#     lng <- input$goto$lng
#     showZipcodePopup(zip, lat, lng)
#     map %>% fitBounds(lng - dist, lat - dist, lng + dist, lat + dist)
#   })
# })

#     output$test_data_filter <- DT::renderDataTable({
#       df_filter <- test_data %>%
#         filter(
#           X_source.intensity_normalized >= input$min_intensity_score,
#           X_source.intensity_normalized <= input$max_intensity_score,
#           is.null(input$rivers) | X_source.river %in% input$rivers,
#           is.null(input$stations) | X_source.station %in% input$stations,
#           is.null(input$sources) | X_source.data_source %in% input$sources
#         ) %>%
#         select(!c("X_source.ms2","X_source.eic", "X_source.rtt")) #%>%
# #        mutate(Action = paste('<a class="go-map" href="" data-lat="', Lat, '" data-long="', Long, '" data-zip="', Zipcode, '"><i class="fa fa-crosshairs"></i></a>', sep=""))
#       action <- DT::dataTableAjax(session, df_filter, outputId = "test_data_filter")
#       
#       DT::datatable(df_filter, options = list(ajax = list(url = action)), escape = FALSE)
#     })


