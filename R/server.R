#server

server <- function(input, output, session, clientData) {
  

##------------------------------------- user log-in/-out  
  
  # call login module supplying data frame, user and password cols and reactive trigger
  credentials <- shinyauthr::loginServer(
    id = "login",
    data = user_base,
    user_col = user,
    pwd_col = password_hash,
    sodium_hashed = TRUE,
    cookie_logins = TRUE,
    sessionid_col = sessionid,
    cookie_getter = get_sessions_from_db,
    cookie_setter = add_session_to_db,
    log_out = reactive(logout_init())
  )
  
  # call the logout module with reactive trigger to hide/show
  logout_init <- shinyauthr::logoutServer(
    id = "logout",
    active = reactive(credentials()$user_auth)
  )
  
  observe({
    if (credentials()$user_auth) {
      shinyjs::removeClass(selector = "body", class = "sidebar-collapse")
    } else {
      shinyjs::addClass(selector = "body", class = "sidebar-collapse")
    }
  })
  
  # display name und logout href ... in div tag
  output$welcome <- renderText({
    req(credentials()$user_auth)
    
    glue("Welcome {user_info()$name}")
  })

##-------------------------------------    
  
  user_info <- reactive({
    credentials()$info
  })
  
  user_data <- reactive({
    req(credentials()$user_auth)
    
    if (user_info()$permissions == "admin") {
      dplyr::starwars[, 1:10]
    } else if (user_info()$permissions == "standard") {
      dplyr::storms[, 1:11]
    }
  })
  

  
  output$testUI <- renderUI({
    req(credentials()$user_auth)
    
    fluidRow(
      column(
        width = 12,
        tags$h4(glue("Your permission level is: {user_info()$permissions}.")),
        tags$h4(glue("You logged in at: {user_info()$login_time}.")),
        tags$h4(glue("Your data is: {ifelse(user_info()$permissions == 'admin', 'Starwars', 'Storms')}.")),
        box(
          width = NULL,
          status = "primary",
          title = ifelse(user_info()$permissions == "admin", "Starwars Data", "Storms Data"),
          DT::renderDT(user_data(), options = list(scrollX = TRUE))
        )
      )
    )
  })
 
#--------------------------------------Start Home-Tab    

  
  
  home_server("create_home")   
  output$home_stuff <- renderUI({
    req(credentials()$user_auth)
    home_ui("create_home") 
  })
  
  
  
#--------------------------------------Start Request-Tab  

  
  
  request_server("get_data", 
                 test_data, 
                 #x_es_fun_list_indices(),
                 x_es_fun_get_data_from_elastic(),
                 func_get_parameters()) #func_get_index(),
  output$request_stuff <- renderUI({
    req(credentials()$user_auth)
  request_ui("get_data", test_data)  
  })

  
  es_glob_df <- func_get_data_data()
  
  observe({
    print("server change df")
    print(es_glob_df)
  })
  

#--------------------------------------Start Dashboard-Tab

    
  
  dashboard_server("create_dashboard", func_get_dashboard_data())
  output$dashboard_stuff <- renderUI({
    req(credentials()$user_auth)
    dashboard_ui("create_dashboard", func_get_dashboard_data()) 
  })

  
  
#--------------------------------------Start Help-Tab
  
  
  
  help_server("create_help")
  output$help_stuff <- renderUI({
    req(credentials()$user_auth)
    help_ui("create_help") 
  })
  
  
  
#--------------------------------------Start Data-Tab  
  
  

  data_server("create_data", func_get_data_data(), es_glob_df)
  output$data_stuff <- renderUI({
    req(credentials()$user_auth)
    data_ui("create_data", func_get_data_data(), es_glob_df)
  })
  
  
}