request_server <- function(id, df){ 
  moduleServer(id, function(input, output, session) {
    data_pre_req <- eventReactive(input$pre_request,{ df }) 
    observe({
      # We'll use these multiple times, so use short var names for
      # convenience.
      index <- input$contin_req_index
      #c_num <- input$control_num
      c_num <- 5
  
      mz_in <- input$inNumber_req_mz
      mz_in_tol <- input$inNumber_req_mz_tol
  
      rtt_in <- input$inNumber_req_rtt
      rtt_in_tol <- input$inNumber_req_rtt_tol
  
  
  
      mz_med <- median(data_pre_req()$X_source.mz)
      mz_min <- min(data_pre_req()$X_source.mz)
      mz_max <- max(data_pre_req()$X_source.mz)
  
      rtt_med <- median(data_pre_req()$rt)
      rtt_min <- min(data_pre_req()$rt)
      rtt_max <- max(data_pre_req()$rt)
  
      method_rt <- unique(data_pre_req()$method)
  
      ufid <- unique(data_pre_req()$X_source.ufid2)
  
      matrix <- unique(data_pre_req()$X_source.matrix)
  
  
  # Number ===================================================
  # Change the label, value, min, and max
  updateNumericInput(session, "inNumber_req_mz",
                     #label = paste("Number ", c_label),
                     value = mz_med, min = mz_min, max = mz_max, step = 0.0005)
  
  updateNumericInput(session, "inNumber_req_rtt",
                     #label = paste("Number ", c_label),
                     value = rtt_med, min = rtt_min, max = rtt_max, step = 0.0005)
  
  # Slider range input =======================================
  # For sliders that pick out a range, pass in a vector of 2
  # values.
  updateSliderInput(session, "inSlider_req_mz",
                    min = mz_min-1,
                    max = mz_max+1,
                    value = c(mz_in-mz_in_tol, mz_in+mz_in_tol))
  
  # An NA means to not change that value (the low or high one)
  updateSliderInput(session, "inSlider_req_rtt",
                    min = rtt_min-1,
                    max = rtt_max+1,
                    value = c(rtt_in-rtt_in_tol, rtt_in+rtt_in_tol))
  
  
  # Date range input =========================================
  # Only label and value can be set for date range input
  updateDateRangeInput(session, "inDateRange",
                       label = paste("Date range", index),
                       start = paste("2013-01-", c_num, sep=""),
                       end = paste("2013-12-", c_num, sep=""),
                       min = paste("2001-01-", c_num, sep=""),
                       max = paste("2030-12-", c_num, sep="")
  )
  
  
  updateSelectInput(session, "ufid", choices = ufid)
  updateSelectInput(session, "inText_req_rtt_method", choices = method_rt)
  updateSelectInput(session, "inText_req_matrix", choices = matrix)
  
  
  # Tabset input =============================================
  # Change the selected tab.
  # The tabsetPanel must have been created with an 'id' argument
  #      if (c_num %% 2) {
  #        updateTabsetPanel(session, "inTabset", selected = "panel2")
  #      } else {
  #        updateTabsetPanel(session, "inTabset", selected = "panel1")
  #      }
})
  })
}
