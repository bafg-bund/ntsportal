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



##-------------------------------------config

##-------------------------------------getdata

json_data <- fromJSON("./Data/cbz_cand.json")
#test_data <- json_data
test_data <- as.data.table(json_data)
test_data <- test_data %>% unnest(X_source.rtt)
test_data <- test_data %>% unnest(X_source.eic)



##-------------------------------------dashboard-data
# dashboard_data <- as.data.table(json_data)
##-------------------------------------filter dashboard
#TO DO na handling, in df

dashboard_data <- as.data.table(fromJSON("./Data/cbz_cand.json")) %>% 
  select( c(Name=X_source.name, # if available
            Formula=X_source.formula, 
            CAS_RN=X_source.cas, 
            Matrix=X_source.matrix, 
            Intensity=X_source.intensity, 
            X_source.rtt, # user needs to select rtt.method with Chrom. Method filter
            Area=X_source.area, 
            Chrom_Meth=X_source.chrom_method,
            Ufid=X_source.ufid, # rm NA's , if available
            mz=X_source.mz, # average mz
            Classification=X_source.comp_group, # rm NA's, as comma sep list
            Area_normalized=X_source.area_normalized,
            Stations=X_source.station,
            lat=X_source.loc.lat,
            lon=X_source.loc.lon,
            Time=X_source.start,
            River=X_source.river
            )) %>% 
  unnest(X_source.rtt) %>%
  rename(tRet=rt,
         Method=method) %>%
  mutate(location=paste0(lat,lon))

# dashboard_data %>% group_by(location) %>% summarise(Doc_count=n())


# summ_data <- dashboard_data %>%
#   group_by(location) %>%
#   reframe(Detections=n(),
#             lon=unique(lon),
#             lat=unique(lat),
#             Name=toString(na.omit(unique(Name))),
#             Formula=toString(na.omit(unique(Formula))),
#             Ufid=toString(na.omit(unique(Ufid))),
#             tRet=mean(tRet),
#             mz=mean(mz),
#             Classification=toString(na.omit(unique(Classification))),
#             Area=median(Area),
#             Area_normalized=median(Area_normalized),
#             Stations=toString(na.omit(unique(Stations))),
#             River=toString(na.omit(unique(River))),
#             Intensity=mean(Intensity)
#             )














