# ui

ui <- dashboardPage(
  
  dashboardHeader(
    title = "bafg",
    disable = FALSE,
    titleWidth = 200,
    #fixed = TRUE,
#    tags$li(
#      class = "dropdown",
#      style = "padding: 8px;",
#      shinyauthr::logoutUI("logout") ######
#    ),
    tags$li(actionLink("openModal", label = "", icon = icon("info")), class = "dropdown")
  ),
  
  dashboardSidebar(
    id= "sidebar",
    collapsed = TRUE,
    width = 200,
    sidebarUserPanel(name=textOutput("welcome"), 
      # userpanel in the side bar -> custom func
      subtitle=logout_link(id = "logout")
      
    ),

    sidebarMenu(
      menuItem("Home", icon = icon("home"), tabName = "home"),
      menuItem("Request", icon = icon("database"), tabName = "request"),
      menuItem("Dashboard", icon = icon("chart-line"), tabName = "dashboard"),
      menuItem("Data", icon = icon("table"), tabName = "data"),
      menuItem("Help", icon = icon("hands-helping"), tabName = "help"),
      menuItem("Generate Report", tabName = "sectors", icon = icon("download"),
               radioButtons('format', 'Document format', c('PDF', 'Word'),inline = FALSE, selected = 1),
               downloadButton("report", "Download Report", class = "butt"),
               tags$head(tags$style(".butt{color: blue !important;}")))
    )
  ),
  
  dashboardBody(
    #----------------------------------login ui
    shinyauthr::loginUI(
      "login", 
      cookie_expiry = cookie_expiry, 
      additional_ui = tagList(
        tags$p("test the different outputs from the sample logins below
             as well as an invalid login attempt.", class = "text-center"),
        HTML(knitr::kable(user_base[, -3], format = "html", table.attr = "style='width:100%;'"))
      )
    ),
    #----------------------------------tabs
    tabItems(
      #----------------------------------home
      tabItem("home",
              uiOutput("home_stuff")
      ),
      #----------------------------------request
      # ui script in R/_Request/request_ui.R
      tabItem("request",
              uiOutput("request_stuff")
              #request_ui("get_data", test_data)        
      ),
      #----------------------------------dashboard
      tabItem("dashboard",
              #dashboard_ui("create_dashboard")
              uiOutput("dashboard_stuff")
      ),
      #----------------------------------data
      tabItem("data",
              uiOutput("data_stuff")
              
      ),
      #----------------------------------help
      tabItem("help",
              #dashboard_ui("create_dashboard")
              uiOutput("help_stuff")
      )
    )
    
  ),
  
  dashboardControlbar()
)