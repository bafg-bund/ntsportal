#global
library(shiny)
library(shinydashboard)
library(shinydashboardPlus)
library(dplyr)
library(glue)
library(shinyauthr)
library(RSQLite)
library(DBI)
library(lubridate)
library(jsonlite)
library(data.table)
library(shinyWidgets)
library(tidyr)
library(elastic)
#library(config)
library(leaflet)
library(markdown)
library(ggplot2)
library(crosstalk)
library(shinybusy)

##-------------------------------------user check
# How many days should sessions last?
cookie_expiry <- 7

# This function must return a data.frame with columns user and sessionid.  Other columns are also okay
# and will be made available to the app after log in.

get_sessions_from_db <- function(conn = db, expiry = cookie_expiry) {
  dbReadTable(conn, "sessions") %>%
    mutate(login_time = ymd_hms(login_time)) %>%
    as_tibble() %>%
    filter(login_time > now() - days(expiry))
}

# This function must accept two parameters: user and sessionid. It will be called whenever the user
# successfully logs in with a password.

add_session_to_db <- function(user, sessionid, conn = db) {
  tibble(user = user, sessionid = sessionid, login_time = as.character(now())) %>%
    dbWriteTable(conn, "sessions", ., append = TRUE)
}

db <- dbConnect(SQLite(), ":memory:")
dbCreateTable(db, "sessions", c(user = "TEXT", sessionid = "TEXT", login_time = "TEXT"))

user_base <- tibble(
  user = c("user1", "user2"),
  password = c("pass", "pass2"),
  password_hash = sapply(c("pass", "pass2"), sodium::password_store),
  permissions = c("admin", "standard"),
  name = c("User One", "User Two")
)




#' logout subtext sidebar module
#'
#' Shiny UI Module for use with \link{logoutServer}
#'
#' @param id An ID string that corresponds with the ID used to call the module's server function
#' @param label label for the logout link
#' @param icon An optional \code{\link[shiny]{icon}} to appear on the button.
#' @param class bootstrap class for the logout text
#' @param style css styling for the logout link
#'
#' @return Shiny UI action text / link
#' @example inst/shiny-examples/basic/app.R
#' @export
logout_link <- function(id, label = "logout", class = "text-success", style = "color: white;") {
  ns <- shiny::NS(id)
  
  shinyjs::hidden(
    shiny::actionLink(ns("button"), label, icon = icon("sign-out"), class = class, style = style)
  )
}


 

#-------------------------------------- get demo data

 func_get_demo_data <- function(){
   dash_data <- as.data.table(fromJSON("./Data/cbz_cand.json"))
   return(dash_data)
 }












