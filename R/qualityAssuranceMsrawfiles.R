# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

# Check quality records ####

validateAllMsrawfiles <- function(records) {
  passed <- all(
    purrr::map_lgl(records, validateRecord),
    noDuplicatedFilenames(records)  
  )
  if (!passed) {
    stop("Validation failed, see warnings")
  } else {
    invisible(TRUE)
  }
}

validateRecord <- function(rec) {
  all(
    fieldsExistForSampleType(rec),
    filesExist(rec),
    correctRawfileLocation(rec$path),
    correctIsTablePolarity(rec),
    correctReplicateRegex(rec),
    correctBlankRegex(rec)
  )
}

noDuplicatedFilenames <- function(records) {
  !any(duplicated(basename(getField(records, "path"))))
}

fieldsExistForSampleType <- function(rec) {
  fieldsFound <- fieldsExist(defineRequiredFieldsAnySample(), rec)
  if (!rec$blank) {
    fieldsFoundEnv <- fieldsExist(defineRequiredFieldsEnvSample(), rec)
    fieldsFound <- append(fieldsFound, fieldsFoundEnv)
  }
  all(fieldsFound)  
}

filesExist <- function(rec) {
  fields <- defineRequiredFilesAnySample()
  all(purrr::map_lgl(fields, checkFileExists, rec = rec))
}

correctRawfileLocation <- function(path) {
  allowedPaths <- c(
    "G2/HRMS/Messdaten",
    "G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files"
  )
  if (any(purrr::map_lgl(allowedPaths, grepl, x = path))) {
    TRUE
  } else {
    warning("File ", path, "not saved in G2/HRMS/Messdaten")
    FALSE
  }
}

correctIsTablePolarity <- function(rec) {
  tablePath <- basename(rec$dbas_is_table)
  if (grepl(rec$pol, tablePath)) {
    TRUE
  } else {
    warning("IS Table does not match polarity in file ", rec$path)
  }
}

correctReplicateRegex <- function(rec) {
  if ("dbas_replicate_regex" %in% names(rec)) {
    all(
      bracketsReplicateRegex(rec$dbas_replicate_regex),
      patternFoundReplicateRegex(rec)
    )
  } else {
    TRUE
  }
}

correctBlankRegex <- function(rec) {
  regexMatch <- grepl(rec$dbas_blank_regex, basename(rec$path))
  isBlank <- rec$blank
  if ((regexMatch && isBlank) || (!regexMatch && !isBlank)) {
    TRUE
  } else {
    warning("File blank regex and filename mismatch: ", rec$path, " and ", rec$dbas_blank_regex)
  }
}

fieldsExist <- function(fields, rec) {
  all(purrr::map_lgl(fields, checkFieldExists, rec = rec))
}

checkFieldExists <- function(field, rec) {
  if (field %in% names(rec)) {
    TRUE
  } else {
    warning(field, " doesn't exist in file ", rec$path)
    FALSE
  }
}

checkFileExists <- function(field, rec) {
  if (file.exists(rec[[field]])) {
    TRUE
  } else {
    warning(field, " in record, ", rec$filename, " with value ", rec[[field]], " not found")
    FALSE
  }
}

bracketsReplicateRegex <- function(replicateRegex) {
  if (grepl("\\(.*\\)", replicateRegex)) {
    TRUE
  } else {
    warning("Brackets not found in ", replicateRegex)
  }
}

patternFoundReplicateRegex <- function(rec) {
  regex <- rec$dbas_replicate_regex
  fileName <- basename(rec$path)
  reducedFileName <- stringr::str_replace(fileName, regex, "\\1")
  if (nchar(reducedFileName) < nchar(fileName)) {
    TRUE
  } else {
    warning("Pattern not found in dbas_replicate_regex for file ", rec$path)
    FALSE
  }
}

defineRequiredFieldsAnySample <- function() {
  c(
    "path",
    "dbas_spectral_library",
    "dbas_is_table",
    "dbas_area_threshold",
    "dbas_rttolm",
    "dbas_mztolu",
    "dbas_mztolu_fine",
    "dbas_ndp_threshold",
    "dbas_rtTolReinteg",
    "date_measurement",
    "dbas_ndp_m",
    "dbas_ndp_n",
    "dbas_instr",
    "pol",
    "chrom_method",
    "matrix",
    "data_source",
    "dbas_index_name",
    "blank",
    "sample_source"
  )
}

defineRequiredFieldsEnvSample <- function() {
  c(
    "duration"
  )
}

defineRequiredFilesAnySample <- function() {
  c(
    "dbas_spectral_library",
    "dbas_is_table",
    "path"
  )
}


# Check batch quality ####
checkQualityBatchList <- function(recordsInBatches) {
  batchesOk <- purrr::map_lgl(recordsInBatches, checkQualityBatch)
  if (!all(batchesOk))
    stop("Quality check batches failed")
  batchesOk
}

checkQualityBatch <- function(records) {
  batchOk <- all(
    allFieldsUniform(records),
    blanksPresent(records),
    enoughSamplesForMinFeatures(records)
  )
  batchOk
}

allFieldsUniform <- function(records) {
  fieldsToCheckAllSamples <- defineFieldsToCheckUniformAllSamples()
  uniformAll <- eachFieldUniform(records, fieldsToCheckAllSamples)
  
  fieldsToCheckEnvSamples <- defineFieldsToCheckUniformEnvSamples()
  recordsEnvSamples <- purrr::discard(records, gf(records, "blank", logical(1)))
  uniformEnv <- eachFieldUniform(recordsEnvSamples, fieldsToCheckEnvSamples) 
  
  uniform <- c(uniformAll, uniformEnv)
  
  if (!all(uniform)) {
    badFields <- names(uniform)[which(!uniform)] 
    for (f in badFields)
      ununiformFieldWarning(records, f)
    FALSE
  } else {
    TRUE
  }
}

blanksPresent <- function(records) {
  if (!any(getField(records, "blank"))) {
    badBatchWarning(records, "No blank found")
    FALSE
  } else {
    TRUE
  }
}

enoughSamplesForMinFeatures <- function(records) {
  minFeat <- getField(records, "nts_alig_filter_min_features")[1]
  if (minFeat > length(records)) {
    badBatchWarning(records, "Not enough samples for minimum features filter")
    FALSE
  } else {
    TRUE
  }
}

eachFieldUniform <- function(records, fields) {
  vapply(fields, fieldUniform, logical(1), records = records)
}

fieldUniform <- function(records, fieldToCheck) {
  if (!fieldPresentAll(records, fieldToCheck))
    return(TRUE)
  allEntries <- getField(records, fieldToCheck)
  length(unique(allEntries)) == 1
}

defineFieldsToCheckUniformAllSamples <- function() {
  c(
    # Sampling
    "duration",
    "matrix",
    
    # Data acquisition
    "pol",
    "data_source",
    "chrom_method",
    
    # dbas Processing
    "dbas_blank_regex",
    "dbas_is_table",
    "dbas_is_name",
    "dbas_index_name",
    "dbas_alias_name",
    "dbas_minimum_detections",
    
    # nts processing
    "nts_spectral_library"
  )
}

defineFieldsToCheckUniformEnvSamples <- function() {
  c(
    # dbas Processing
    "dbas_replicate_regex",
    "dbas_build_averages",
    
    # nts processing
    "nts_alig_filter_min_features",
    "nts_alig_filter_type"
  )
}

ununiformFieldWarning <- function(records, field) {
  badBatchWarning(records, "field not uniform")
  warning("Error in field ", field)
}

badBatchWarning <- function(records, reason) {
  batchName <- dirname(getField(records, "path")[1])
  warning(reason, " in batch ", batchName)
}









# Integrity checks ####


check_integrity_msrawfiles_nts <- function(msrawfilesDocsList) {
  
  dl <- msrawfilesDocsList
  ds <- lapply(dl, "[[", i = "_source")
  # takes too long
  #df <- data.table::rbindlist(lapply(ds, as.data.frame), fill = T)
  

  # Fields that must be present in all docs
  fieldsPresentAll <- c(
    "nts_mz_step",
    "nts_spectral_library"
  )
  purrr::walk(ds, function(doc) {
    purrr::walk(fieldsPresentAll, function(x) {
      if (!is.element(x, names(doc)))
        stop("Field ", x, " not found in doc ", doc$path)
    })
  })

  # Check that files exist
  pth1 <- unique(gf(ds, "nts_spectral_library", character(1)))
  pth2 <- gf(ds, "path", character(1))
  if (!all(file.exists(pth1, pth2)))
    stop("Not all files found")
  
  # Check that nts_mz_min is always lower than nts_mz_max
  
  mzCheck <- all(
    as.numeric(
      purrr::compact(
        sapply(ds, "[[", i = "nts_mz_min")
        )
      ) < 
      as.numeric(
        purrr::compact(
          sapply(ds, "[[", i = "nts_mz_max")
        )
      )
  )
  if (!mzCheck) {
    stop("nts_mz_min is not always lower than nts_mz_max")
  }
 
  invisible(TRUE)
}




#' Check consistency of msrawfiles DB
#' 
#' Must be done before any processing operation
#'
#' @param escon Connection object created with `elastic::connect`
#' @param rfindex Elasticsearch index name for msrawfiles index
#' @param locationRf Root directory for all rawfiles
#'
#' @return TRUE if successful (invisibly)
#' @export
#'
check_integrity_msrawfiles <- function(escon, rfindex, locationRf) {
  
  locationRf <- normalizePath(locationRf)
  
  # Check presence of spectral library
  resp2 <- elastic::Search(escon, rfindex, body = '
                {
  "query": {
    "match_all": {}
  },
  "aggs": {
    "csl_loc": {
      "terms": {
        "field": "dbas_spectral_library",
        "size": 100
      }
    }
  },
  "size": 0
}
                ')
  
  buc <- resp2$aggregations$csl_loc$buckets
  cslp <- sapply(buc, function(x) x$key)
  stopifnot(all(file.exists(cslp)), all(grepl("\\.db$", cslp)))
  for (pth in cslp) {
    dbtest <- DBI::dbConnect(RSQLite::SQLite(), pth)
    if (!DBI::dbExistsTable(dbtest, "experiment"))
      stop("DB not of the correct format")
    DBI::dbDisconnect(dbtest)
  }
  
  # Check presence of IS tables
  resp3 <- elastic::Search(escon, rfindex, body = '
  {
    "query": {
      "match_all": {}
    },
    "aggs": {
      "ist_loc": {
        "terms": {
          "field": "dbas_is_table",
          "size": 100
        }
      }
    },
    "size": 0
  }
  ')
  
  buc <- resp3$aggregations$ist_loc$buckets
  istp <- sapply(buc, function(x) x$key)
  stopifnot(all(file.exists(istp)), all(grepl("\\.csv$", istp)))
  
  # Check that certain fields exist in all docs
  stopifnot(
    check_field(escon, rfindex, "dbas_is_table"),
    check_field(escon, rfindex, "dbas_area_threshold"),
    check_field(escon, rfindex, "dbas_rttolm"),
    check_field(escon, rfindex, "dbas_mztolu"),
    check_field(escon, rfindex, "dbas_mztolu_fine"),
    check_field(escon, rfindex, "dbas_ndp_threshold"),
    check_field(escon, rfindex, "dbas_rtTolReinteg"),
    check_field(escon, rfindex, "date_measurement"),
    check_field(escon, rfindex, "dbas_ndp_m"),
    check_field(escon, rfindex, "dbas_ndp_n"),
    check_field(escon, rfindex, "dbas_instr"),
    check_field(escon, rfindex, "pol"),
    check_field(escon, rfindex, "chrom_method"),
    check_field(escon, rfindex, "matrix"),
    check_field(escon, rfindex, "data_source"),
    check_field(escon, rfindex, "dbas_index_name"),
    check_field(escon, rfindex, "blank"),
    check_field(escon, rfindex, "path"),
    check_field(escon, rfindex, "sample_source"),
    check_field(escon, rfindex, "duration", onlyNonBlank = TRUE)
  )
  
  # Check that blank files do not have coordinates
  checkLoc <- elastic::Search(escon, rfindex, body = '
                {
  "query": {
    "bool": {
      "must_not": [
        {
          "exists": {
            "field": "loc"
          }
        }
      ],
      "must": [
        {
          "term": {
            "blank": {
              "value": false
            }
          }
        }
      ]
    }
  }
}
                ')
  stopifnot(checkLoc$hits$total$value == 0)
  
  # Collect rawfiles ####
  resp <- es_search_paged(escon, rfindex, searchBody = '
  {
    "query": {
      "match_all": {}
    },
  "_source": ["path"]
  }
', sort = "path:asc")
  
  
  hits <- resp$hits$hits
  allFls <- data.frame(
    id = sapply(hits, function(x) x[["_id"]]),
    path = sapply(hits, function(x) x[["_source"]]$path)
  )
  
  fileCheck <- sapply(allFls$path, file.exists)
  if (!all(fileCheck)) {
    log_warn("The following files do not exist")
    noFile <- allFls$path[!fileCheck]
    for (i in noFile)
      message(i)
    stop("Missing raw files")
  }
  
  allFls$dir <- dirname(allFls$path)
  allFls$base <- basename(allFls$path)
  
  # there can not be any duplicated filenames, because the filename is used
  # by the alignment to distinguish between files.
  # but in blanks it's okay, these will not factor into alignment anyway
  
  if (any(duplicated(allFls$base))) {
    idsDup <- allFls[duplicated(allFls$base), "id"]
    isBlank <- elastic::docs_mget(
      escon, rfindex, ids = idsDup, source = "blank", verbose = FALSE
    )$docs
    if (all(sapply(isBlank, function(d) d[["_source"]][["blank"]]))) {
      log_warn("There are duplicated filenames in the db (but only for blanks)")
    } else {
      stop("There are duplicated filenames in the db")  
    }
  }
  
  # Check that there are no mismatches between IS table and polarity
  polcheck_body <- function(pols) {
    sprintf('
  {
    "query": {
      "bool": {
        "must": [
          {
            "regexp": {
              "dbas_is_table": ".*%s.*"
            }
          },
          {
            "regexp": {
              "filename": ".*%s.*"
            }
          },
          {
            "term": {
              "pol": "%s"
            }
          }
        ]
      }
    }
  }             
  ', pols[1], pols[2], pols[3])
  }
  check_polarity_consitency <- function(pols) {
    elastic::Search(escon, rfindex, body = polcheck_body(pols))$hits$total$value == 0
  }
  
  # Get all permutations of 3 polarities
  perms <- gtools::permutations(2, 3, c("pos", "neg"), repeats.allowed = T)[2:7,]
  polCheck <- apply(perms, 1, check_polarity_consitency)
  if (any(!polCheck)) {
    stop("Inconsistencies in the polarity of filename, pol and dbas_is_table,
         use this query to find them:", 
         apply(perms[!polCheck, , drop = F], 1, polcheck_body))
  }
  
  # Check that all files are located in correct location. If not, give a warning.
  resm <- elastic::Search(escon, rfindex, source = "path", size = 10000, body = sprintf('
    {
      "query": {
        "bool": {
          "must_not": [
            {
              "regexp": {
                "path": "%s.*"
              }
            }
          ]
        }
      }
    }
  ', locationRf))
  if (resm$hits$total$value > 0) {
    badp <- vapply(resm$hits$hits, function(x) x[["_source"]]$path, character(1))
    tx <- paste(unique(dirname(badp)), collapse = "\n")
    log_warn("{length(badp)} files are not saved in HRMS/Messdaten, see directories:\n {tx}")
  }
  
  # Check that 
  check_dbas_replicate_regex(escon, rfindex)
  check_dbas_blank_regex(escon, rfindex)
  
  invisible(TRUE)
}



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
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.
