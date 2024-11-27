#' Ingest of json files into Elasticsearch
#'
#' @param json_path Path to single json file or directory with json files.
#'
#' @return Returns TRUE when completed (invisibly)
#' @export
#'
ingestJson <- function(json_path) {
  
  # py_install("keyring")
  # py_install("elasticsearch")
  # py_install("keyrings.alt")
  # py_install("SecretStorage")
  # py_install("dbus-python")
  # py_install('pandas')
  # reticulate::py_install('tqdm')
  # reticulate::py_install('pyyaml')
  
  # Load ingest module
  reticulate::source_python(fs::path_package("ntsportal", "scripts", "ingest_main.py"))
  
  # Run the main ingest function
  all_index_alias_pairs <- ingest(json_path)
  
  return(all_index_alias_pairs)
}
