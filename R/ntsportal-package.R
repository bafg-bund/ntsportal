# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal If not, see <https://www.gnu.org/licenses/>.

#' @keywords internal 
"_PACKAGE"

#' ntsportal: A package for non-target data management
#'
#'
#' @name ntsportal
#' @import dplyr
#' @import logger
#' @import future
#' @import ntsworkflow
#' @import ggplot2
#' @import rlang
NULL



.onAttach <- function(libname, pkgname) {
  packageStartupMessage("\nCreating escon variable for connection to elasticsearch\n")
  
  if (haveCred()) {
    createEscon()
  } else if (!haveRing()) {
    createRing()
    setCred()
    createEscon()
  } else {
    setCred()
    createEscon()
  }
}

createEscon <- function(ring = "ntsportal") {
  if (keyring::keyring_is_locked(ring))
    keyring::keyring_unlock(ring, ring)
  ntspCred <- getCred(ring)
  escon <<- elastic::connect(
    host = 'elastic.dmz.bafg.de', 
    port = 443, 
    user = ntspCred[1], 
    pwd  = ntspCred[2],
    transport_schema = "https",
    ssl_verifypeer = F
  )
  
  if (escon$ping()$cluster_name != "bfg-elastic-cluster") {
    warning("Connection to Elasticsearch cluster not established")
  }
}

haveCred <- function(ring = "ntsportal") {
  keyNames <- c("ntsportal-user", "ntsportal-pwd")
  
  if (haveRing(ring)) {
    allKeys <- keyring::key_list(keyring = ring)$service
  } else {
    return(FALSE)
  }
  
  all(keyNames %in% allKeys)
}

haveRing <- function(ring = "ntsportal") {
  # Bug in keyring, there must be a "system" keyring
  if (!is.element("system", keyring::keyring_list()$keyring))
    keyring::keyring_create("system", password = "system")
  
  allRings <- keyring::keyring_list()$keyring
  is.element(ring, allRings)
}

getCred <- function(ring = "ntsportal") {
  c(
    usr = keyring::key_get("ntsportal-user", keyring = ring),
    pwd = keyring::key_get("ntsportal-pwd", keyring = ring)
  )
}

setCred <- function(ring = "ntsportal", usr = character(), pwd = character()) {
  if (keyring::keyring_is_locked(keyring = ring))
    keyring::keyring_unlock(keyring = ring, password = ring)  
  
  if (length(usr) > 0 && length(pwd) > 0) {
    # Non interactive usage
    keyring::key_set_with_value("ntsportal-user", keyring = ring, password = usr)
    keyring::key_set_with_value("ntsportal-pwd", keyring = ring, password = pwd)  
  } else {
    # Interactive usage
    keyring::key_set("ntsportal-user", keyring = ring, prompt = "Username: ")
    keyring::key_set("ntsportal-pwd", keyring = ring, prompt = "Password: ")
  }
}

createRing <- function(ring = "ntsportal") {
  keyring::keyring_create(keyring = ring, password = ring)  # keyring and keyring password are the same
}
