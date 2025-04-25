# Copyright Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

#' Add new records for MS measurement files to msrawfiles index
#'
#' @param rfindex index name for rawfiles index
#' @param templateId ES-ID for an existing msrawfiles-record to use as a template
#' @param newPaths Character vector of full paths to new rawfiles which are to be added, must be mzXML files.
#' @param newStart Either "filename" (default), meaning extract the start time from the filename or provide a start date
#'   as an 8 digit number "YYYYMMDD"
#' @param newStation Either be "same_as_template" (default), which will copy the value from the template document, or
#'   "filename", meaning extract the station and loc values from the code in the filename (will compare to other docs in
#'   msrawfiles index) or "newStationList" to add a new station. See details.
#' @param dirMeasurmentFiles Root directory where original measurement files are located. The function will look for original
#'   (vendor) files in this directory or below. Must end with "/".
#' @param promptBeforeIngest Should the user be asked to verify the submission? (Default: TRUE). A file called
#'   "add-rawfiles-check.json" created in the saveDirectory which the user can check and make changes to. See details
#'   "making manual changes".
#' @param saveDirectory Location to save temporary json file for checking and making manual changes. (defaults to
#'   current working dir)
#' @param newStationList List with fields "station" and "loc" and optionally "river", "gkz" and "km" to add a new
#'   station. see details
#'   
#' @details \strong{Adding location information (`station` and `loc` fields)}
#'   newStation can be either "same_as_template", "filename" or "newStationList".
#'
#'   Using "same_as_template" means the station and location information is copied from the template
#'
#'   Using "filename" means it will use the `dbas_station_regex` field in the index to get the station information from
#'   another record in the index. The function will use the regex to extract the station code from the filename and
#'   search the msrawfiles index for other records with this station code. If an unambiguous location is found, it will
#'   gather all available location information and use this for the new record entry. This option is useful if the batch
#'   that is being added contains more than one station.
#'
#'   Using "newStationList" means a new fixed station name and location is passed (use list in the argument
#'   "newStationList" with fields "station" and "loc" and optionally "river", "gkz" and "km") "loc" being a geopoint
#'   with fields "lat" and "lon". Station and river names should all be lowercase with no spaces and no special
#'   characters. For the station name use the convention <river_town_position> where position is l, r or m for left,
#'   right, middle. If there is no obvious town but km is known, then use the convention <river>_<km> for the station
#'   name. If neither town nor km is known, use the convention <river>_<description> where description is some
#'   indication of the location.
#' 
#'   \strong{Adding sample time information (`start` field)}
#'   
#'   Using "filename" means that the sample time is extracted from the filename. The field `dbas_date_regex` is used to
#'   find the date of the sample from the file name. It works in conjunction with `dbas_date_format`. `dbas_date_regex`
#'   extracts the text, while `dbas_date_format` tells R how to interpret the text. For example, the file name
#'   `RH_pos_20170101.mzXML` uses the date_regex `"([20]*\\d{6})"` and date_format `"ymd"`. The `dbas_date_regex` uses
#'   the tidyverse regular expression syntax and the `stringr::str_match` function to extract the text referring to the
#'   date. The brackets indicate the text to extract and these can be surrounded by anchors. It is also possible to have
#'   multiple brackets, the text in multiple brackets will be combined before parsing. For example the file
#'   `UEBMS_2024_002_Main_Kahl_Jan_pos_DDA.mzXML` can be parsed with date_regex `_(20\\d{2})_.*_(\\w{3})_pos` and
#'   date_format `ym`.
#'
#'   `dbas_date_format` may be one of `"ymd"`, `"dmy"`, `"ym"` for year-month and `"yy"` for just the year. The date
#'   parsing is done by `lubridate`.
#'   
#'   \strong{Making manual changes}
#'   
#'   Manual changes can be made to the json but only if one document is added (this is to minimize errors). Select "c"
#'   at the promt (after changes to the json have been saved).
#'   
#'   \strong{File storage locations}
#'   
#'   The location of where the mzXML files are stored is given in the argument `newPaths`.`saveDirectory` is where the json
#'   file is written to. `dirMeasurmentFiles` is only used to look for the original (non-converted) vendor files (e.g.,
#'   wiff files) to read the measurement time (which is not copied into the mzXML file using some converters). Depending
#'   on the number of files being uploaded, the function can be very slow because the function will look through a large
#'   directory. To improve performance, you can tell the function to look in a smaller directory. If no files are found
#'   then the function will add the creation time of the mzXML file.
#'   
#' @examples
#' \dontrun{
#' library(ntsportal)
#' source("~/connect-ntsp.R")
#' rfindex <- "ntsp_msrawfiles"
#' paths <- list.files("/beegfs/nts/ntsportal/msrawfiles/ulm/schwebstoff/dou_pos/", "^Ulm.*mzXML$", full.names = TRUE)
#' templateId <- findTemplateId(rfindex, blank = FALSE, pol = "pos", station = "donau_ul_m", matrix = "spm")
#' addRawfiles(rfindex = rfindex, templateId = templateId, newPaths = paths)
#' checkMsrawfiles()
#' 
#' # Files in batch have different sample location
#' addRawfiles(rfindex, "eIRBnYkBcjCrX8D7v4H5", newFiles[4:17], newStation = "filename", 
#' dirMeasurmentFiles = "~/messdaten/sachsen/") 
#' 
#' # Addition of a new station
#' addRawfiles(
#'   rfindex, "eIRBnYkBcjCrX8D7v4H5", newFiles[15], 
#'   newStation = "newStationList",
#'   newStationList = list(
#'     station = "pleisse_9",
#'     loc = list(lat = 51.251489, lon = 12.383758),
#'     river = "pleisse",
#'     km = 9,
#'     gkz = 5666
#'   )
#' ) 
#' }
#' 
#' @return Returns vector of new ES-IDs of the generated and imported documents
#' @export
#'
addRawfiles <- function(
    rfIndex, 
    templateId, 
    newPaths, 
    newStart = "filename",
    newStation = "same_as_template",
    dirMeasurmentFiles = "/srv/cifs-mounts/g2/G/G2/HRMS/Messdaten/",
    promptBeforeIngest = TRUE,
    saveDirectory = getwd(),
    newStationList = list(
      station = "example_new_station",
      loc = list(lat = 1.23456, lon = 1.23456),
      river = "example_new_river",
      gkz = 99999,
      km = 99999
    )
) {
  
  checkArgumentValidity(rfIndex, templateId, newPaths, newStart, newStation, newStationList)
  
  newPaths <- removePreviouslyAdded(rfIndex, newPaths)
  
  if (length(newPaths) == 0) {
    stop("No files to add")
  }
  
  message("The files\n", paste(basename(newPaths), collapse = "\n"), 
          "\n will be added")
  
  templateRec <- getTemplateRecord(rfIndex, templateId)
  checkTemplateRecord(newPaths, templateRec, promptBeforeIngest)
  if (newStart == "filename")
    checkDateParsing(newPaths, templateRec, promptBeforeIngest)
  if (newStation == "filename")
    checkStationParsing(newPaths, templateRec)
  
  newRecords <- lapply(
    newPaths, 
    newRecordFromTemplate, 
    newStart = newStart, 
    newStation = newStation,
    template = templateRec,
    rfIndex = rfIndex, 
    newStationList = newStationList,
    dirMeasurmentFiles = dirMeasurmentFiles
  )
  
  jsonlite::write_json(newRecords, file.path(saveDirectory, "add-rawfiles-check.json"), pretty = T, 
                       digits = NA, auto_unbox = T)
  
  message("Please check ", file.path(saveDirectory, "add-rawfiles-check.json"))
  
  message("Is it okay to proceed? 
          y = will be uploaded 
          n = terminate process without uploading 
          c = json was changed manually. Only 1 doc allowed. Save changes before proceeding.")
  if (promptBeforeIngest)
    isOk <- readline("(y/n/c): ") else isOk <- "y"
  
  switch(
    isOk,
    y = NULL,
    n = stop("Aborting."),
    c = {
      if (length(newRecords) != 1)
        stop("You can only change one document and use this as template for others")
      message("Adding changed document")
      newRecordsNew <- jsonlite::read_json("add-rawfiles-check.json")
      if (all.equal(newRecordsNew, newRecords)[1] == TRUE)
        stop("There was no change made, aborting.")
      newRecords <- newRecordsNew
    },
    stop("Unknown input, aborting.")
  )
  
  ids <- character()
  for (doci in newRecords) {
    response <- elastic::docs_create(escon, rfIndex, body = doci)
    ids <- append(ids, response[["_id"]])
  }
  idString <- paste(shQuote(ids, type = "cmd"), collapse = ", ")
  idString <- paste0(idString, "\n")
  message("Documents were uploaded with the IDs")
  message(idString)
  
  # ElasticSearch needs time for indexing
  Sys.sleep(2)  

  timesFound <- vapply(newPaths, filenameCountInIndex, numeric(1), rfIndex = rfIndex)
  
  if (any(timesFound == 0)) {
    notCreated <- newPaths[timesFound == 0]
    warning("The files ", paste(notCreated, collapse = ", "), " were not created.")
  } else {
    message("Function completed, all files in index ", rfIndex)  
  }
  
  ids
}

checkArgumentValidity <- function(rfIndex, templateId, newPaths, newStart, newStation, newStationList) {
  checkResult <- TRUE
  
  if (length(templateId) != 1) {
    checkResult <- addError(checkResult, "Only one templateId allowed")
  }
  
  if (!grepl("^ntsp.*msrawfiles.*", rfIndex)) {
    checkResult <- addError(checkResult, "Index name is incorrect or does not follow convention")
  }
  
  fileFound <- vapply(newPaths, file.exists, logical(1))
  
  if (!all(fileFound)) {
    checkResult <- addError(checkResult, "Files not found")
  }
  
  if (anyDuplicated(newPaths)) {
    checkResult <- addError(checkResult, "There are duplicated filepaths")
  }
  
  fileKind <- vapply(newPaths, grepl, pattern = "\\.mzX?ML$", logical(1))
  if (!all(fileKind)) {
    checkResult <- addError(checkResult, "Files are not all .mzXML or .mzML files")
  }
  
  filePol <- getPolFromPaths(newPaths)
  if (length(filePol) != 1) {
    checkResult <- addError(checkResult, "Only one polarity allowed per batch")
  }
  
  if (!(newStart == "filename" || grepl("^\\d{8}$", newStart))) {
    checkResult <- addError(checkResult, "'newStart' must be 'filename' or a fixed date in the form 'yyyymmdd'")
  }
  
  if (!is.element(newStation, c("same_as_template", "filename", "newStationList"))) {
    checkResult <- addError(checkResult, "newStation must be 'same_as_template' or 'filename' or a list 
            with the elements station, loc and optionally river and km")
  } 
  
  if (!validateStationList(newStationList)) {
    checkResult <- addError(checkResult, "'newStationList' is malformed, check documentation")
  }
  
  stopIfAnyErrors(checkResult)
}

checkTemplateRecord <- function(newPaths, templateRec, promptBeforeIngest) {
  checkResult <- TRUE
  if (templateRec$blank)
    message("Note: Template document is a blank file")
  
  polFromFilename <- getPolFromPaths(newPaths)
  if (templateRec$pol != polFromFilename) {
    message("New file(s) do not have the same polarity as the template, only 
            one file may be added")
    if (length(newPaths) > 1 || !promptBeforeIngest)
      checkResult <- addError(checkResult, "More than one file added with different polarity to template")
  }
  
  dateFormat <- templateRec$dbas_date_format
  if (!is.element(dateFormat, c("ymd", "dmy", "yy", "ym"))) {
    message("Unknown dbas_date_format ", dateFormat, " in template")
    if (length(newPaths) > 1 || !promptBeforeIngest)
      checkResult <- addError(checkResult, "More than one file added with invalid dbas_date_format in template")
  }
  
  if (!validateRecord(templateRec)) {
    checkResult <- addError(checkResult, "validateRecord failed for template")
  }
  
  stopIfAnyErrors(checkResult)
}



checkDateParsing <- function(newPaths, templateRec, promptBeforeIngest) {
  checkResult <- TRUE
  newDates <- vapply(
    basename(newPaths),
    getStartFromFilenameAsDate,
    lubridate::ymd(19700101),
    dateRegex = templateRec$dbas_date_regex,
    dateFormat = templateRec$dbas_date_format
  )
  if (any(is.na(newDates))) {
    badFiles <- newPaths[is.na(newDates)]
    badFilesString <- paste(badFiles, collapse = ", ")
    if (length(newPaths) == 1 && promptBeforeIngest) {
      message("File ", badFilesString, " produced an NA start date, changed to 1970-01-01, must be corrected")
    } else {
      checkResult <- addError(checkResult, paste("File(s)", badFilesString, "produced NA start date(s)"))
    }
  }

  stopIfAnyErrors(checkResult)
}

checkStationParsing <- function(newPaths, templateRec) {
  if (!is.element("dbas_station_regex", names(templateRec)))
    stop("dbas_station_regex, not found in template")
}


newRecordFromTemplate <- function(pth, newStart, newStation, template, rfIndex, newStationList, dirMeasurmentFiles) {
  rec <- template
  rec <- addPathToRecord(rec, pth)
  rec <- addPolToRecord(rec)
  rec <- addStartToRecord(rec, newStart)
  rec <- addStationToRecord(rec, newStation, rfIndex, newStationList)
  rec <- addMeasurementTimeToRecord(rec, dirMeasurmentFiles)
  
  rec$date_import <- as.integer(Sys.time())
  rec$filesize <- file.size(rec$path) / 1e6
  rec <- rec[order(names(rec))]
  rec <- newMsrawfilesRecord(rec)
  if (!validateRecord(rec))
    warning("Record validation failed for file ", pth)
    
  rec
}

addPathToRecord <- function(rec, pth) {
  rec$path <- normalizePath(pth)
  rec$filename <- basename(pth)
  rec
}

addPolToRecord <- function(rec) {
  rec$pol <-  getPolFromFilename(rec$filename)
  rec
}

addStartToRecord <- function(rec, newStart) {
  if (newStart == "filename") {
    rec$start <- getFormatedStartFromFilename(rec$filename, rec$dbas_date_regex, rec$dbas_date_format)
  } else {
    rec$start <- getFixedStart(newStart)
  }
  rec
}

getFormatedStartFromFilename <- function(fname, dateRegex, dateFormat) {
  
  newDate <- getStartFromFilenameAsDate(fname, dateRegex, dateFormat)
  
  if (is.na(newDate))
    newDate <- lubridate::ymd(19700101) 
  
  reformatDate(newDate)
}

getStartFromFilenameAsDate <- function(fname, dateRegex, dateFormat) {
  dateString <- paste(stringr::str_match(fname, dateRegex)[,-1], collapse = "_")
  
  switch(
    dateFormat,
    yy = lubridate::ymd(dateString, tz = "Europe/Berlin", truncated = 2),
    ym = lubridate::ymd(dateString, tz = "Europe/Berlin", truncated = 1),
    ymd = lubridate::ymd(dateString, tz = "Europe/Berlin"),
    dmy = lubridate::dmy(dateString, tz = "Europe/Berlin")
  )
}

getFixedFormatedStart <- function(newStart) {
  newDate <- lubridate::ymd(newStart, tz = "Europe/Berlin", truncated = 2)
  reformatDate(newDate)
}

reformatDate <- function(dateObject) {
  format(dateObject, "%Y-%m-%d", tz = "Europe/Berlin")
}

addStationToRecord <- function(rec, newStation, rfIndex, newStationList) {
  # In the case of "same_as_template" no changes are made to rec (it was a copy of template)
  if (newStation == "filename") {
    
    stationList <- getStationListFromCode(rfIndex, rec$filename, rec$dbas_station_regex)
    rec <- appendStationListFields(rec, stationList)
  } else if (newStation == "newStationList") {
    rec <- appendStationListFields(rec, newStationList)
  } 
  rec
}

getStationListFromCode <- function(rfIndex, filename, stationRegex) {
  stationQueryRegex <- makeStationQueryRegex(filename, stationRegex)
  stationHits <- getHitsWithStation(rfIndex, stationQueryRegex)
  
  checkStationIsUnique(stationHits)
  checkLocIsUnique(stationHits)
  
  stationList <- list(station = st[1], loc = locs[[1]])
  stationList <- addOptionalStationField("river", stationList, stationHits)
  stationList <- addOptionalStationField("km", stationList, stationHits)
  stationList <- addOptionalStationField("gkz", stationList, stationHits)
  
  stopifnot(validateStationList(stationList))
  stationList
}

validateStationList <- function(stationList) {
  is.list(stationList) && 
    length(stationList) >= 2 && 
    all(c("station", "loc") %in% names(stationList))
}

appendStationListFields <- function(rec, stationList) {
  
  rec$station <- stationList$station
  rec$loc <- stationList$loc
  
  if ("river" %in% names(stationList)) {
    rec$river <- stationList$river
  } else {
    rec$river <- NULL
  }
  
  if ("km" %in% names(stationList)) {
    rec$km <- stationList$km
  } else {
    rec$km <- NULL
  }
  
  if ("gkz" %in% names(stationList)) {
    rec$gkz <- stationList$gkz
  } else {
    rec$gkz <- NULL
  }
  rec
}

makeStationQueryRegex <- function(filename, stationRegex) {
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
  regex3 <- gsub("\\\\", "\\\\\\\\", regex2)
  regex3
}

getHitsWithStation <- function(rfIndex, stationQueryRegex) {
  res <- elastic::Search(escon, rfIndex, body = sprintf('
  {
  "query": {
      "regexp": {
        "filename": "%s"
      }
    },
    "size": 10000,
    "_source": ["station", "loc", "river", "km", "gkz"]
  }
  ', stationQueryRegex))
  
  stopifnot(res$hits$total$relation == "eq")
  if (res$hits$total$value == 0) {
    stop("No documents found with the station code: ", stationCode, 
         " for filename ", filename)
  }
  res$hits$hits
}

checkStationIsUnique <- function(stationHits) {
  stationsInHits <- vapply(stationHits, function(doc) doc[["_source"]][["station"]], character(1))
  if (length(unique(stationsInHits)) != 1)
    stop("More than one station found in ntsportal")
}

checkLocIsUnique <- function(stationHits) {
  locsInHits <- lapply(hits, function(doc) doc[["_source"]][["loc"]])
  testLocs <- outer(locsInHits, locsInHits, Vectorize(all.equal))
  if (is.list(testLocs)) {
    message("Not all locs for station ", st[1], " are exactly the same")
    message(paste(unlist(testLocs)[unlist(testLocs) != "TRUE"], collapse = "\n"))
    isOk <- readline("Continue anyway? (y/n): ")
    switch(
      isOk, 
      y = {
        message(
          "Taking the first loc ", 
          paste(unlist(locs[[1]]), collapse = ", "),
          " for station ", st[1])
      },
      n = {
        stop("Processing terminated.")
      },
      {
        stop("Could not understand input.")
      }
    )
  }
}

addOptionalStationField <- function(field, resListTemp, hits) {
  if (all(vapply(hits, function(doc) !is.null(doc[["_source"]][[field]]), logical(1)))) {
    vals <- vapply(hits, function(doc) doc[["_source"]][[field]], 
                   switch(field, river = character(1), km = numeric(1), gkz = numeric(1)))
    if (length(unique(vals)) != 1) {
      stop("Not all ", field, " are the same for station ", st[1])
    }
    resListTemp[[field]] <- vals[1]
  }
  resListTemp
}

addMeasurementTimeToRecord <- function(rec, dirMeasurmentFiles) {
  nm <- stringr::str_match(basename(rec$path), "^(.*)\\.mzX?ML$")[,2]
  fd <- list.files(dirMeasurmentFiles, pattern = nm, recursive = T, full.names = T)
  if (length(fd) > 0) {
    timeString <- format(min(file.mtime(fd)), "%Y-%m-%d %H:%M:%S")
  } else {
    warning("Measurement time not found, using creation date of mzXML file")
    timeString <- format(min(file.mtime(pathMsFile)), "%Y-%m-%d %H:%M:%S")
  }
  if (!is.na(timeString) && timeString != "") {
    rec$date_measurement <- timeString
  }
  rec
}

removePreviouslyAdded <- function(rfIndex, newPaths) {
  newPaths[filesNotInIndex(rfIndex, newPaths)]
}

filesNotInIndex <- function(rfIndex, newPaths) {
  vapply(newPaths, fileNotInIndex, logical(1), rfIndex = rfIndex)
}

fileNotInIndex <- function(pathToCheck, rfIndex) {
  fileCount <- filenameCountInIndex(pathToCheck, rfIndex)
  if (fileCount == 1)
    warning(rfIndex, " already contains ", pathToCheck)
  if (fileCount > 1)
    warning(rfIndex, " contains duplicates in ", pathToCheck, ".\n Please correct.")
  fileCount == 0
}

filenameCountInIndex <- function(pathToCheck, rfIndex) {
  res <- elastic::Search(
    escon, rfIndex, size = 0, 
    body = list(query = list(term = list(filename = basename(pathToCheck))))
  )
  res$hits$total$value
}

getPolFromPaths <- function(newPaths) {
  unique(vapply(basename(newPaths), getPolFromFilename, character(1)))
}

getPolFromFilename <- function(fname) {
  stringr::str_extract(fname, "pos|neg")
}

addError <- function(checkResult, warningText) {
  warning(warningText, call. = FALSE)
  c(checkResult, FALSE)
}

stopIfAnyErrors <- function(checkResult) {
  if (any(checkResult == FALSE)) {
    message("Error in arguments, function will terminate")
  }
  
  if (!all(checkResult))
    stop("Error in addRawfiles, see warnings for details")
  invisible(all(checkResult))
}

getTemplateRecord <- function(rfIndex, templateId) {
  res <- elastic::docs_get(escon, rfIndex, templateId, verbose = F)
  newMsrawfilesRecord(res$`_source`)
}

