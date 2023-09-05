
#' Get all esids from an index
#' 
#' The maximum allowed number of return ids is 10000. This function will
#' hang if there are more than 10000 docs in the index
#'  
#' @param escon 
#' @param rfindex 
#'
#' @return character vector of IDs
#'
get_all_ids <- function(escon, rfindex) {
  res <- elastic::Search(escon, rfindex, body = '{
      "query": {
        "match_all": {}
      },
      "size": 10000,
      "_source": ["_id"]
    }')
  stopifnot(res$hits$total$relation == "eq")
  vapply(res$hits$hits, "[[", i = "_id", character(1))
}

#' Check if file is m
#'
#' @param escon 
#' @param rfindex 
#'
#' @return
rawfiles_missing <- function(escon, rfindex) {
  allIds <- get_all_ids(escon, rfindex)
  allPths <- get_field(allIds, "path")
  notExist <- vapply(allPths, Negate(file.exists), logical(1))
  allPths[notExist]
}

#' Normalize all paths in the msrawfiles index
#'
#' @param escon 
#' @param rfindex 
#'
#' @return no return value
#' @export
#'
norm_rf_paths <- function(escon, rfindex) {
  # get all ids
  allIds <- get_all_ids(escon, rfindex)
  # idi <- allIds[100]
  for (idi in allIds) { 
    # get path
    whichId <- which(idi == allIds)
    if (whichId %% 100 == 0)
      message("Working on ", whichId, " of ", length(allIds))
    resi <- elastic::docs_get(escon, rfindex, idi, source = "path", verbose = F)
    pthi <- resi$`_source`$path
    if (!file.exists(pthi)) {
      warning("File ", pthi, " does not exist.")
      next
    }
    newPthi <- normalizePath(pthi)
      
    resi2 <- elastic::docs_update(escon, rfindex, idi, body = sprintf('
                                                                      {
        "script": {
          "source": "ctx._source.path = params.newPath",
          "params": {
            "newPath": "%s"
          }
        }
      }
    ', newPthi))
    if (resi2$result != "updated")
      warning("In id ", idi, " doc not updated")
  }
}


#' Get station and location based on othe docs in msrawfiles index
#' 
#' @param escon 
#' @param rfindex 
#' @param filename 
#' @param stationRegex Must have brackets for stringr::str_match 
#'
#' @return list with geolocation and station name
station_from_code <- function(escon, rfindex, filename, stationRegex) {
  # Unique identifier for the station
  stationCode <- stringr::str_match(filename, stationRegex)[,2]
  
  # Convert regex for Query DSL
  regex2 <- stationRegex
  if (grepl("^\\^", stationRegex)) {
    regex2 <- sub("^\\^", "", regex2)
  } else {
    regex2 <- paste0(".*", regex2)
  }
  if (grepl("\\$$", stationRegex)) {
    regex2 <- sub("\\$$", "", regex2)
  } else {
    regex2 <- paste0(regex2, ".*")
  } 
  regex2 <- sub("\\(.*\\)", stationCode, regex2)
  
  # Find which other docs have this identifier
  
  res <- elastic::Search(escon, rfindex, body = sprintf('
                                                        {
  "query": {
    "regexp": {
      "filename": "%s"
    }
  },
  "size": 10000,
  "_source": ["station", "loc"]
}
', regex2))
  
  stopifnot(res$hits$total$relation == "eq")
  if (res$hits$total$value == 0)
    stop("No documents found with the station code: ", stationCode)
  # Check that there is no ambiguity
  hits <- res$hits$hits
  st <- vapply(hits, function(doc) doc[["_source"]][["station"]], character(1))
  stopifnot(length(unique(st)) == 1)
  locs <- lapply(hits, function(doc) doc[["_source"]][["loc"]])
  stopifnot(all(outer(locs, locs, Vectorize(all.equal))))
  
  list(station = st[1], loc = locs[[1]])
}

#' Add new MS measurement files to msrawfiles index
#' 
#' @param escon 
#' @param rfindex 
#' @param templateId 
#' @param newPaths 
#' @param newStart Either "filename" (default), meaning extract the start time 
#' from the filename or provide a start date as an 8 digit number "YYYYMMDD"
#' @param newStation Either be "same_as_template" (default), which will
#' copy the value from the template document, or "filename", meaning extract
#' the station and loc values from the code in the filename (will compare to 
#' other docs in msrawfiles index) or provide a list of length 2 with station 
#' and loc values. See details.
#'
#' @details newStation can be either "same_as_template" or "filename" or
#' a new fixed station name and location (list with fields "station" and "loc")
#' "loc" being a geopoint with fields "lat" and "lon"
#' if it is in the filename it will use the dbas_station_regex field in
#' the index to get the station information from another doc in the index
#' 
#' @return
#' @export
#'
add_rawfiles <- function(escon, rfindex, templateId, newPaths, 
                         newStart = "filename", 
                         newStation = "same_as_template") {
  
  fileFound <- vapply(newPaths, file.exists, logical(1))
  if (!all(fileFound))
    stop("Files not found")
  # for now newStart must be filename or a new start date
  stopifnot(newStart == "filename" || grepl("^\\d{8}$", newStart))
  stopifnot(newStation %in% c("same_as_template", "filename") || is.list(newStation))
  
  if (is.list(newStation)) {
    stopifnot(
      length(newStation) == 2, 
      all(c("station", "loc") %in% names(newStation)),
      is.character(newStation$station),
      length(newStation$station) == 1,
      all(c("lat", "lon") %in% names(newStation$loc)),
      all(vapply(newStation$loc, is.numeric, logical(1)))
    )
  }
  
  #Check if any duplicates in filenames
  check_files <- function(pths, isBlank = FALSE) {
    vapply(pths, function(pth) {
      if (!file.exists(pth))
        return(FALSE)
      pth <- normalizePath(pth)
      checkValue <- ifelse(isBlank, pth, basename(pth))
      checkField <- ifelse(isBlank, "path", "filename")
      resx <- elastic::Search(escon, rfindex, body = sprintf('{
        "query": {
          "term": {
            "%s": {
              "value": "%s"
            }
          }
        },
        "size": 0
      }', checkField, checkValue))
      if (resx$hits$total$value > 1)
        stop("There are duplicates of ", pth, " in ", rfindex)
      resx$hits$total$value == 1
    }, logical(1))
  }
  
  stopifnot(length(templateId) == 1)
  
  # Get template doc
  
  res <- elastic::docs_get(escon, rfindex, templateId, verbose = F)
  templDoc <- res$`_source`
  
  if (templDoc$blank)
    message("Template document is a blank file")
  
  # Check that the files do not already exist
  # If the file is not a blank, the basename must be unique, otherwise, the
  # path must be unique
  # The file paths are normalized before comparison
  checkBefore <- if (templDoc$blank) 
    check_files(newPaths, isBlank = T) else check_files(newPaths)
  
  if (any(checkBefore)) {
    alreadyPresent <- names(checkBefore[checkBefore])
    message("The files\n", paste(alreadyPresent, collapse = "\n"), 
            "\nare already present and will not be added")
    newPaths <- newPaths[!checkBefore]
  }
  
  # Copy template and change some values
  newDocs <- lapply(newPaths, function(pth) {
    doc <- templDoc
    doc$path <- normalizePath(pth)
    doc$filename <- basename(pth)
    poli <- stringr::str_extract(doc$filename, "[posneg]{3}")
    #browser()
    stopifnot(doc$pol == poli) 
    
    # All blanks must also have a start from now on. 
    # TODO at the moment only dates can be read. This function must also
    # work for ymd_hms datetimes.
    if (newStart == "filename") {
      date_string <- stringr::str_match(doc$filename, doc$dbas_date_regex)[,2]
      temp <- lubridate::ymd(date_string, tz = "Europe/Berlin", truncated = 2)
      doc$start <- format(temp, "%Y-%m-%d", tz = "Europe/Berlin")
    } else {
      temp <- lubridate::ymd(newStart, tz = "Europe/Berlin", truncated = 2)
      doc$start <- format(temp, "%Y-%m-%d", tz = "Europe/Berlin")
    }
    
    if (is.na(doc$start))
      stop("File ", pth, " produced an NA start date")
    
    if (newStation == "filename") {
      stopifnot("dbas_station_regex" %in% names(doc))
      staList <- station_from_code(
        escon = escon, 
        rfindex = rfindex, 
        filename = doc$filename,
        stationRegex = doc$dbas_station_regex
      )
      if (is.list(staList) && length(staList) == 2 && 
          all(c("station", "loc") %in% names(staList))) {
        doc$station <- staList$station
        doc$loc <- staList$loc  
      } else {
        stop("Station parsing in file ", pth, " failed")
      }
    } else if (is.list(newStation)) {
      doc$station <- newStation$station
      doc$loc <- newStation$loc
    }
    
    doc$date_import <- as.integer(Sys.time())
    doc
  })
  
  jsonlite::write_json(newDocs, "add-rawfiles-check.json", pretty = T, digits = NA, auto_unbox = T)
  
  message("Please check ", file.path(getwd(), "add-rawfiles-check.json"))
  
  isOk <- readline("Is it okay to proceed? (y/n): ")
  if (isOk != "y") {
    stop("processing stoped, files not added")
  } 
  
  for (doci in newDocs)
    elastic::docs_create(escon, rfindex, body = doci)
  
  Sys.sleep(10)
  # check that everything has been entered
  
  checkAfter <- check_files(newPaths)
  
  if (!all(checkAfter)) {
    notCreated <- names(checkAfter[!checkAfter])
    warning("The files ", paste(notCreated, collapse = ", "), " were not created.")
  } else {
    message("Function completed, all files in index ", rfindex)  
  }
  
}

#' Get ID based on search parameters
#'
#' @param escon 
#' @param rfindex 
#' @param isBlank default is FALSE
#' @param polarity 
#' @param station 
#' @param matrix default is spm
#'
#' @return string templateID
#' @export
#'
find_templateid <- function(escon, rfindex, isBlank = FALSE, polarity, station, matrix = "spm") {
    tempID <- elastic::Search(escon, rfindex, body = sprintf('
                                                   {
  "query": {
    "bool": {
      "must": [
        {
          "term": {
             "station": {
              "value": "%s"
            }
          }
        },
        {
          "term": {
            "pol": {
              "value": "%s"
            }
          }
        },
        {
          "term": {
            "matrix": {
              "value": "%s"
            }
          }
        },
        {
          "term": {
            "blank": {
              "value": %s
            }
          }
        }
      ]
    }
  },
  "_source": false,
  "size": 1
}

', station, polarity, matrix, ifelse(isBlank, "true", "false") ))
    if (tempID$hits$total$value == 0) {
      warning("search have no hits")
      return(NULL)
    }
      tempID$hits$hits[[1]]$`_id`
}



