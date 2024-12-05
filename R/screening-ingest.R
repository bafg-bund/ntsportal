# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

#' Ingest of json.gz files into Elasticsearch
#' @description
#' Ingest json.gz files into Elasticsearch
#' 
#' @details
#' Required python packages:
#' reticulate::py_install('elasticsearch')
#' reticulate::py_install('pandas')
#' reticulate::py_install('tqdm')
#' reticulate::py_install('pyyaml')
#' 
#' @param json_path Path to single json file or directory with json files.
#'
#' @return Returns pairs for all unique aliases and the respective indeces
#' @export
#'
ingestJson <- function(json_path) {
  
  # Load python ingest module
  reticulate::source_python(fs::path_package("ntsportal", "scripts", "ingest_main.py"))
  
  # Path for index mappings
  mapping_path <- fs::path_package("ntsportal", "inst", "extdata")
  
  # Run the main ingest function
  all_index_alias_pairs <- ingest(json_path, mapping_path)
  
  return(all_index_alias_pairs)
}
