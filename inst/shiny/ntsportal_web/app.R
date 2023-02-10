# NTSportal App
# written by: Kevin Jewell
# Last update: 17.09.2021

library(shiny)
library(shinyBS)
library(DT)
library(ggplot2)
library(shinymanager)
#library(ntsworkflow)



#### UI ####
ui <- fluidPage(
  sidebarLayout(
    #### Index and Filters ####
    sidebarPanel(
      width = 2,
      actionButton("credInput", "Login"),
      textInput("index", "Index regex", "g2_nts_bfg")
      ),
    mainPanel(width = 10,
      tabsetPanel(
        type = "tabs",
        tabPanel(
          "Features",
          fluidRow(
            DT::dataTableOutput("featureTable")
          )
        ),
        tabPanel(
          "Alignment",
          DT::dataTableOutput("alignmentTable")
        ),
        tabPanel(
          "debug",
          verbatimTextOutput("pingTest")
        )
      )
    )
  )
)

server <- function(input, output, session) {
  
  # load settings
  appSet <- yaml::read_yaml("ntsp-app-settings.yml")
  
  # connect to server ####
  # hold login credentials
  creds <- reactiveValues(username = NULL, password = NULL)
  escon <- reactiveVal()
  loginModal <- function(failed = FALSE) {
    modalDialog(
      textInput("uname", "User:", placeholder = 'firstname.lastname'),
      passwordInput("pwd", "Password:"),
      span('Please enter username and password for elasicsearch'),
      if (failed)
        div(tags$b("Connection not possible, invalid credentials or bad connection", style = "color: red;")),
      
      footer = tagList(
        modalButton("Cancel"),
        actionButton("okLogin", "OK")
      )
    )
  }
  
  observeEvent(input$credInput, {
    showModal(loginModal())
  })
  
  observeEvent(input$okLogin, {
    # Check that login worked
    #browser()
    validText <- !is.null(input$uname) && nzchar(input$uname) && 
      !is.null(input$pwd) && nzchar(input$pwd)
    if (validText) {
      escon(
        elastic::connect(
          host = appSet$host, 
          user=input$uname, 
          pwd=input$pwd,
          port = 443,
          transport_schema = "https"
        )
      )
      if (escon()$ping()$cluster_name == "bfg-elastic-cluster") {
        removeModal()
      } else {
        showModal(loginModal(failed = TRUE))
      }
      
    } else {
      showModal(loginModal(failed = TRUE))
    }
  })
  
  output$pingTest <- renderText({
    unlist(escon()$ping())
  })
  
  #### Feature tab ####
  output$featureTable <- DT::renderDataTable({
    # get features
    #browser()
    res <- elastic::Search(escon(), input$index, body = '
    {
      "query": {
        "match_all": {}
      },
      "size": 10000,
      "_source": ["mz", "rt", "name"]
    }                       
    ')
    b <- res$hits$hits
    df <- data.frame(
      mz = sapply(b, function(x) x$`_source`$mz),
      rt = sapply(b, function(x) x$`_source`$rt),
      name = sapply(b, function(x) {
        n <- x$`_source`$name
        if (is.null(n))
          "" else paste(n, collapse = ", ")
        })
    )
    DT::datatable(df)
  })
  
  #### Alignment tab ####
  output$alignmentTable <- DT::renderDataTable({
    # get features grouped by ufid
    #browser()
    res <- elastic::Search(escon(), input$index, body = '
       {
        "query": {
          "match_all": {}
        },
        "size": 0,
        "aggs": {
          "ufids": {
            "terms": {
              "field": "ufid",
              "size": 100000
            },
            "aggs": {
              "mzStat": {
                "extended_stats": {
                  "field": "mz"
                }
              },
              "rtStat": {
                "extended_stats": {
                  "field": "rt"
                }
              },
              "names": {
                "terms": {
                  "field": "name",
                  "size": 10
                }
              }
            }
          }
        }
      }
    ')
    b <- res$aggregations$ufids$buckets
    df <- data.frame(
      ufid = vapply(b, function(x) x$key, integer(1)),
      mean_mz = vapply(b, function(x) x$mzStat$avg, numeric(1)),
      mean_rt = vapply(b, function(x) x$rtStat$avg, numeric(1)),
      names = vapply(b, function(x) {
        b2 <- x$names$buckets
        if (length(b2) > 0) {
          n <- vapply(b2, function(y) y$key, character(1))
          paste(n, collapse = ", ")
        } else {
          ""
        }
      }, character(1))
    )
    DT::datatable(df)
  })
  
}

shinyApp(ui = ui, server = server)


