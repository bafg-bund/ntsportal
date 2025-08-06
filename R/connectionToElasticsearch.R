

#' Set ElasticSearch credentials and create ElasticSearch connection object 
#' @description 
#' NTSPortal credentials are used to build the DbComm interface to the database. 
#' If no credentials are available a pop-up will ask for
#' user input (in interactive use). Uses the `keyring` package to 
#' store and retrieve credentials. The default `keyring` backend is used.
#' When working on Linux in the console, this is the 'file'
#' backend, on windows, 'wincred'.
#' The backend can be set manually with `Sys.setenv("R_KEYRING_BACKEND" = "file")`.
#' @details Will additionally create the `escon` connection object in the 
#' global environment for use by all functions (deprecated, use DbComm interface)
#' @export
connectNtsportal <- function() {
  checkSystemRing()  # strange behavior in keyring, there must be a system ring
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

#' Create connection with new credentials
#' @description For use when stored creditials need to be changed.
#' @export
resetConnectionCredentials <- function() {
  clearRing()
  connectNtsportal()
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
  unlockRing()
  keyring::key_set("ntsportal-user", keyring = ring, prompt = "Username for NTSPortal: ")
  keyring::key_set("ntsportal-pwd", keyring = ring, prompt = "Password for NTSPortal: ")
}

#' Set credentials for accessing NTSPortal ElasticSearch
#' @description For progammatically setting the credentials. 
#' @param ring Keyring name, do not change the default
#' @param usr username, enter manually for non-interactive use
#' @param pwd password, enter manually for non-interactive use
#' @export
setCredNonInteractive <- function(ring = "ntsportal", usr = character(), pwd = character()) {
  checkSystemRing() # Bug in keyring, there must be a system ring
  if (!haveRing(ring = ring)) {
    createRing(ring = ring)
  } 
  unlockRing()
  if (length(usr) > 0 && length(pwd) > 0) {
    # Non interactive usage
    keyring::key_set_with_value("ntsportal-user", keyring = ring, password = usr)
    keyring::key_set_with_value("ntsportal-pwd", keyring = ring, password = pwd)  
  }
}

unlockRing <- function(ring = "ntsportal") {
  if (keyring::keyring_is_locked(keyring = ring))
    keyring::keyring_unlock(keyring = ring, password = ring)  
}

createRing <- function(ring = "ntsportal") {
  keyring::keyring_create(keyring = ring, password = ring)  # keyring and keyring password are the same
}

checkSystemRing <- function() {
  if (!haveRing("system"))
    createRing("system")
}

clearRing <- function(ring = "ntsportal") {
  if (is.element(ring, keyring::keyring_list()$keyring)) {
    keyring::keyring_delete(ring)
  } else {
    warning(ring, " keyring not found")
  }
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal