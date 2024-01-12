# Functions to add data to documents in NTSPortal

#' Add rtt field to all documents where this does not exist
#'
#'
#'
#' @param escon connection to ElasticSearch
#' @param esindex Index or index-pattern to update
#'
#' @return Number of updated docs (invisibly)
#' @export
#'
es_add_rtt <- function(escon, esindex) {
  # Run script to add rtt to any docs where this does not exist.
  # check that it does not exist

  checkRes <- elastic::Search(escon, index = esindex, body = '
  {
    "query": {
      "nested": {
        "path": "rtt",
        "query": {
          "bool": {
            "must_not": [
              {
                "exists": {
                  "field": "rtt.rt"
                }
              }
            ]
          }
        }
      }
    },
    "size": 0
  }
  ')

  totalNoRtt <- checkRes$hits$total$value

  if (totalNoRtt > 0) {
    res <- elastic::docs_update_by_query(escon, index = esindex, body = '
  {
    "query": {
      "bool": {
        "filter": [
          {
            "exists": {
              "field": "rt"
            }
          },
          {
            "exists": {
              "field": "chrom_method"
            }
          }
        ],
        "must_not": [
          {
            "nested": {
              "path": "rtt",
              "query": {
                "term": {
                  "rtt.predicted": {
                    "value": false
                  }
                }
              }
            }
          }
        ]
      }
    },
    "script": {
      "source" : "
        params.entry[0].put(\'rt\', ctx._source[\'rt\']);
        params.entry[0].put(\'method\', ctx._source[\'chrom_method\']);
        if (ctx._source.rtt != null) {
          ctx._source.rtt.add(params.entry);
        } else {
          ctx._source.rtt = params.entry;
        }
      ",
      "params" : {
        "entry" : [
          {
            "predicted" : false
          }
        ]
      }
    }
  }')

    log_info("Completed update on {res$updated} docs")
    invisible(res$updated)
  } else {
    log_info("No docs to update.")
    invisible(0L)
  }
}


#' Add rt_clustering field to es index pattern
#'
#' @param escon connection to ElasticSearch
#' @param esindex Index or index-pattern to update
#'
#' @return NULL
#' @export
#'
#' @import logger
es_add_rt_cluster <- function(escon, esindex) {
  # get all features without rt_clustering
  totUpdated <- 0
  repeat {
    res <- elastic::Search(escon, index, body = '
     {
      "query": {
        "bool": {
          "must_not": [
            {
              "exists": {
                "field": "rt_clustering"
              }
            }
          ]
        }
      },
      "_source": ["rtt"],
      "size": 10000
    }

                       ')
    if (res$hits$total$value == 0) {
      break
    }

    log_info("There are {res$hits$total$value} left to process")

    fts <- res$hits$hits
    for (ft in fts) {
      # get rt
      doc <- ft[["_source"]]
      esid <- ft[["_id"]]
      esind <- ft[["_index"]]
      rtToCopy <- doc$rtt[[which(sapply(doc$rtt, function(x) x$method == "bfg_nts_rp1"))]]$rt
      elastic::docs_update(escon, esind, esid, body = sprintf('
    {
      "script" : {
        "source": "ctx._source.rt_clustering = params.rtToCopy;",
        "params": {
          "rtToCopy": %.2f
        }
      }
    }
    ', rtToCopy))
    }
    totUpdated <- sum(totUpdated, res$hits$total$value)
  }

  log_info("Completed all features")
  invisible(totUpdated)
}


#' Get list of all compounds in an ntsp index
#'
#' Option to add a query to limit the documents
#' @param escon 
#' @param index 
#' @param addQuery query content in the form of a list.
#'
#' @return
#'
#' @examples 
#' \dontrun{
#' termsSearch <- list(
#'   terms = list(
#'     filename = as.list(filenames)
#'   )
#' )
#' 
#' comps <- es_get_comps(escon, index, termsSearch)
#' }
#' 
#' 
es_get_comps <- function(escon, index, addQuery = NULL) {
  aggsList <- list(
    comps = list(
      terms = list(
        field = "name",
        size = 100000
      )
    )
  )
  if (is.null(addQuery)) {
    r <- elastic::Search(escon, index, body = list(aggs = aggsList), size = 0)
  } else {
    r <- elastic::Search(
      escon, index, body = list(query = addQuery, aggs = aggsList), size = 0
    )
  }
  vapply(r$aggregations$comps$buckets, function(x) x$key, character(1))
}


#' Add compound classification data from spectral library to ntsp dbas index
#'
#' The dbas index will be updated with 
#'
#' @param escon
#' @param sdb connection to spec lib
#' @param index
#' @param filenames Limit the update to results documents from specific files defined by 'filename' field.
#' If 'all' (default) all dbas documents are updated. 
#'
#' @return True (invisibly)
#' @export
#' @import dplyr
#' @import logger
es_add_comp_groups <- function(escon, sdb, index, filenames = "all") {
  # allowed comp groups, as they are currently formated in spectral-lib
  # ntsp uses all lower case
  COMPGROUPS <- c(
    "Pharmaceutical",
    "Transformation_product",
    "Antimicrobial",
    "Food_additive",
    "Fungicide",
    "Herbicide",
    "Industrial_process",
    "Insecticide",
    "Metabolite",
    "Natural_product",
    "Personal_care_product",
    "Pesticide",
    "Pigment"
  )

  log_info("{length(COMPGROUPS)} allowed groups: {paste(COMPGROUPS, collapse = ', ')}")

  # check that allowed comp groups are all in database
  testGroups <- tbl(sdb, "compoundGroup") %>% collect()
  if (!all(COMPGROUPS %in% testGroups$name)) {
    stop("not all allowed comp groups are found in db")
  }

  # first delete current compound groups, making sure there is no duplication

  log_info("Removing current comp_group field")
  
  # build query body
  queryBody = list(
    bool = list(
      must = list(
        list(
          exists = list(
            field = "comp_group"
          )
        )
      )
    )
  )
  
  if (filenames[1] != "all") {
    termsSearch <- list(
      terms = list(
        filename = as.list(filenames)
      )
    )
    res <- elastic::Search(escon, index, body = list(query = termsSearch), size = 0)
    if (res$hits$total$value == 0) {
      log_info("es_add_comp_groups: nothing to update")
      return(FALSE)
    }
    queryBody$bool$must <- append(queryBody$bool$must, list(termsSearch))
  } 
  
  # Build update body
  updateBod <- list(
    query = queryBody,
    script = list(
      source = "ctx._source.remove(\'comp_group\')",
      lang = "painless"
    )
  )
  
  res <- elastic::docs_update_by_query(escon, index, body = updateBod)
  
  if (res$timed_out) {
    stop("Unsuccessful removal of comp_group field")
  }

  log_info("Removed comp_group from {res$total} docs, adding new comp_group")
  rm(res)
  
  # Addition of compound groups
  # Get list of all compounds in db
  
  if (filenames[1] != "all") {
    comps <- es_get_comps(escon, index, termsSearch)
  } else {
    comps <- es_get_comps(escon, index)
  }

  # Function to get compound groups from sdb for each compound
  get_groups <- function(compName) {
    compt <- tbl(sdb, "compound")
    cg <- tbl(sdb, "compoundGroup")
    cgc <- tbl(sdb, "compGroupComp")
    groups <- filter(compt, name == compName) %>%
      select(compound_id) %>%
      left_join(cgc, by = "compound_id") %>%
      select(compoundGroup_id) %>%
      left_join(cg, by = "compoundGroup_id") %>%
      select(name) %>%
      collect() %>%
      unlist() %>%
      unname()
    groups <- groups[groups %in% COMPGROUPS]
    groups <- tolower(groups)
    groups
  }

  compGroups <- lapply(comps, get_groups)
  names(compGroups) <- comps
  compGroups <- Filter(function(x) length(x) != 0, compGroups)

  log_info("Updating comp_group on {length(compGroups)} compounds")

  # Loop through each compound and add classifications to elastic documents

  for (cp in names(compGroups)) {
    for (gr in compGroups[[cp]]) {
      tryCatch(
        elastic::docs_update_by_query(
          escon, index, refresh = "true", 
          body = sprintf(
          '
          {
            "query": {
              "term": {
                "name": {
                  "value": "%s"
                }
              }
            },
            "script": {
              "source": "
                if (ctx._source.comp_group == null) {
                  ctx._source.comp_group = params.newGroup;
                } else {
                  ctx._source.comp_group = [ctx._source.comp_group];
                  ctx._source.comp_group.add(params.newGroup);
                }
              ",
              "lang": "painless",
              "params": {
                "newGroup": "%s"
              }
            }
          }
          ', cp, gr
          )
        ),
        error = function(cnd) {
          log_error("Updated group {gr} failed for comp {cp}")
        }
      )
    }
  }
  log_info("Completed es_add_comp_groups on index {index}")
  invisible(TRUE)
}

#' Add formula, inchi and inchikey to docs in ntsp
#'
#' @param escon
#' @param sdb
#' @param index
#' @param filenames Limit the update to results documents from specific files defined by 'filename' field.
#' If 'all' (default) all dbas documents are updated. 
#'
#' @return
#' @export
#' @import dplyr
es_add_identifiers <- function(escon, sdb, index, filenames = "all") {
  ctb <- tbl(sdb, "compound") %>%
    select(name, formula, inchi, inchikey, SMILES) %>%
    collect()

  # Get all compound names
  if (filenames[1] != "all") {
    termsSearch <- list(
      terms = list(
        filename = as.list(filenames)
      )
    )
    res <- elastic::Search(escon, index, body = list(query = termsSearch), size = 0)
    if (res$hits$total$value == 0) {
      logger::log_warn("es_add_identifiers: nothing to update")
      return(FALSE)
    }
    allComps <- es_get_comps(escon, index, termsSearch)
  } else {
    allComps <- es_get_comps(escon, index)
  }

  # For each comp, get formula and other data

  fdf <- data.frame(
    name = allComps,
    formula = vapply(allComps, function(x) ctb[ctb$name == x, "formula", drop = TRUE], character(1)),
    inchi = vapply(allComps, function(x) ctb[ctb$name == x, "inchi", drop = TRUE], character(1)),
    inchikey = vapply(allComps, function(x) ctb[ctb$name == x, "inchikey", drop = TRUE], character(1)),
    smiles = vapply(allComps, function(x) ctb[ctb$name == x, "SMILES", drop = TRUE], character(1))
  )
  rownames(fdf) <- NULL
  
  # Remove any rows with NA values, this data must be added to spec lib
  if (any(is.na(fdf))) {
    logger::log_warn("There are NA values in spectral library for formula, \
                     inchi, inchikey or smiles")
    narow <- apply(
      fdf[, c("formula", "inchi", "inchikey", "smiles")],
      1,
      function(x) any(is.na(x))
    )
    fdf <- fdf[!narow, ]
  }
  
  for (i in seq_len(nrow(fdf))) {
    queryPart <- list(
      term = list(
        name = fdf[i, "name"]
      )
    )
    if (filenames[1] != "all") {
      queryPart <- list(
        bool = list(
          must = list(
            queryPart,
            termsSearch
          )
        )
      )
    }
    scriptPart <- list(
      source = "ctx._source.formula = params.form; ctx._source.inchi = params.inchi; ctx._source.inchikey = params.inchikey; ctx._source.smiles = params.smiles;",
      params = list(
        form = fdf[i, "formula"],
        inchi = fdf[i, "inchi"],
        inchikey = fdf[i, "inchikey"],
        smiles = fdf[i, "smiles"]
      )
    )
    tryCatch(
      elastic::docs_update_by_query(
        escon, index, body = list(query = queryPart, script = scriptPart)
      ),
      error = function(cnd) {
        logger::log_error(
          "In es_add_identifiers for \
          compound {fdf[i, 'name']} in index {index} \
          with message {conditionMessage(cnd)}"
        )
      }
    )
  }
  invisible(TRUE)
}
