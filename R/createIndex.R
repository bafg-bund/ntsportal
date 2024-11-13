# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal


createNewIndexForResults <- function(escon, mappingType = "nts", indexName) {
  f <- switch(
    mappingType,
    nts = fs::path_package("ntsportal", "extdata", "nts_index_mappings.json"),
    dbas = fs::path_package("ntsportal", "extdata", "dbas_index_mappings.json"),
    spectral_library = fs::path_package("ntsportal", "extdata", "spectral_library_index_mappings.json"),
    stop("unknown mapping type")
    )
  mappings <- jsonlite::read_json(f)
  elastic::index_create(escon, indexName, body = mappings)
}

