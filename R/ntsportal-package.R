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
  packageStartupMessage("\nBefore starting, use connectNtsportal() to create connection object to ElasticSearch\n")
}

#' Create ElasticSearch connection object
#' 
#' @description
#' If credentials are available, will create the "escon" connection object in the 
#' global environment for use by all functions. Otherwise will ask for input of
#' credentials (in interactive use).
#' 
#' @export
#'
connectNtsportal <- function() {
  checkSystemRing() # Bug in keyring, there must be a system ring
  if (haveCred()) {
    createEscon()
  } else if (!haveRing() && interactive()) {
    createRing()
    setCred()
    createEscon()
  } else if (interactive()) {
    setCred()
    createEscon()
  } else {
    warning("Please run connectNtsportal interactively, in order to set the credentials for accessing NTSPortal")
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
    rm(escon)
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
  allRings <- keyring::keyring_list()$keyring
  is.element(ring, allRings)
}

getCred <- function(ring = "ntsportal") {
  c(
    usr = keyring::key_get("ntsportal-user", keyring = ring),
    pwd = keyring::key_get("ntsportal-pwd", keyring = ring)
  )
}

setCred <- function(ring = "ntsportal") {
  keyring::key_set("ntsportal-user", keyring = ring, prompt = "Username for NTSPortal: ")
  keyring::key_set("ntsportal-pwd", keyring = ring, prompt = "Password for NTSPortal: ")
}

createRing <- function(ring = "ntsportal") {
  keyring::keyring_create(keyring = ring, password = ring)  # keyring and keyring password are the same
}


checkSystemRing <- function() {
  if (!haveRing("system"))
    createRing("system")
}

#' Set credentials for accessing NTSPortal ElasticSearch
#'
#' @param ring Keyring name, do not change the default
#' @param usr username, enter manually for non-interactive use
#' @param pwd password, enter manually for non-interactive use
#'
#' @export
#'
setCredNonInteractive <- function(ring = "ntsportal", usr = character(), pwd = character()) {
  checkSystemRing() # Bug in keyring, there must be a system ring
  if (!haveRing(ring = ring)) {
    createRing(ring = ring)
  } 
  if (keyring::keyring_is_locked(keyring = ring))
    keyring::keyring_unlock(keyring = ring, password = ring)  
  if (length(usr) > 0 && length(pwd) > 0) {
    # Non interactive usage
    keyring::key_set_with_value("ntsportal-user", keyring = ring, password = usr)
    keyring::key_set_with_value("ntsportal-pwd", keyring = ring, password = pwd)  
  }
}

clearRing <- function(ring = "ntsportal") {
  if (is.element(ring, keyring::keyring_list()$keyring)) {
    keyring::keyring_delete(ring)
  } else {
    warning(ring, " keyring not found")
  }
}
