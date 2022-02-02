# this file loads the report, extracts the data and produces the json file 
# it normalizes intensities and builds averages from replicate injections
# dbas4.R is used to import json files to Elastic
# usage: Rscript dbas3.R dbas_settings.yaml blah_i.report
# only one report can be processed at a time!

library(ntsworkflow)

# erlaubte Stations
station <- c("donau_jo_m","donau_ul_m","elbe_ba_m","elbe_bl_m","elbe_cu_m",
             "elbe_tan_l","elbe_pr_r","elbe_ze_l","rhein_bi_l","rhein_if_m",
             "rhein_ko_l","rhein_we_r","saar_gu_m","saar_re_m") 
# erlaubte Polarität
pol <- c("pos","neg")

norm_ms2 <- function(x, precursormz, mztol = 0.02, noiselevel = 0.1) {
  # remove precursor and noise
  x <- x[x$mz < (precursormz - mztol), ]  # remove precursor otherwise MS1 checked again
  x <- x[x$int >= noiselevel, ]
  if (length(x) == 0 || nrow(x) == 0)
    return(NULL)
  data.frame(mz = x$mz, int = x$int / max(x$int))
}

norm_ms1 <- function(x, precursormz, precursorInt, noiselevel = 0.1) {
  # remove noise
  x <- x[x$int >= noiselevel, ]
  if (length(x) == 0 || nrow(x) == 0)
    return(NULL)
  data.frame(mz = x$mz, int = x$int / precursorInt)
}


# Load settings and processed and integrated report file ####

stopifnot(length(commandArgs(trailingOnly = TRUE)) == 2)

settings <- yaml::read_yaml(commandArgs(trailingOnly = TRUE)[1]) 

reportFile <- commandArgs(trailingOnly = TRUE)[2] 


stopifnot(grepl("_i\\.report$", reportFile))  # it must be an already processed report file

dbas <- ntsworkflow::loadReport(F, reportFile)  


message("Collecting data...")
# collect data for elasticsearch 
compData <- dbas$integRes[, c("samp", "comp_name", "int_a")]
compData$samp <- basename(compData$samp)

if (!is.null(settings$is_table)) {
  isData <- dbas$ISresults[dbas$ISresults$IS == settings$is_auswahl, c("samp", "int_a")]
  # normalize intensities
  dat <- merge(compData, isData, by = "samp", suffix = c("", "_IS"))
  # verify columns are numeric
  dat[, c("int_a", "int_a_IS")] <- lapply(dat[, c("int_a", "int_a_IS")], as.numeric)
  dat$norm_a <- round(dat$int_a / dat$int_a_IS, 4)
} else {
  dat <- compData
  dat$int_a_IS <- NA
  dat$norm_a <- NA
}

if (length(dat$int_a_IS) == 0)
  stop("IS not found, if no IS spiked, remove table from the settings")

if (!all(is.na(dat$int_a_IS))) {
  stdev <- round((sd(dat$int_a_IS)), 2)
  message("Die SD des internen Standards beträgt ", stdev)
  if  (stdev >= 100) 
    warning("Die Standardabweichung des internen Standards ist über 100")
}

# build average area from replicates ####
if (!is.null(settings$replicate_regex)) {
  message("Building averages...")
  # using the regex, give all replicate samples the same name (best when replicates are indicated by _1 at the end)
  dat$reps <- stringr::str_replace(dat$samp, settings$replicate_regex, "\\1")
  averages <- by(dat, list(dat$reps, dat$comp_name), function(part) {
    comp1 <- part$comp_name[1]
    samp1 <- part$samp[1]
    average_int_a <- mean(part$int_a, na.rm = T)
    standard_deviation_norm_a <- sd(part$norm_a, na.rm = T)
    average_norm_a <- mean(part$norm_a, na.rm = T)
    average_int_a_IS <- mean(part$int_a_IS, na.rm = T)
    data.frame(samp = samp1, comp_name = comp1, int_a = average_int_a,
               int_a_IS = average_int_a_IS, norm_a = average_norm_a, sd_norm_a = standard_deviation_norm_a)
  }, simplify = F)
  dat <- do.call("rbind", averages)
  # warning if sd for any comp is high
  if (any((dat$sd_norm_a >= 10), na.rm = T))   {
    message("Die SD einer Substanz ist eventuell zu hoch")
    probleme <- subset(dat, sd_norm_a >= 10, comp_name, drop = TRUE)
    probleme_unique <- unique(probleme)
    message("Problematische Substanzen:")
    for (x in probleme_unique) 
      message(x)
    warning("Überprüfung Reportdatei notwendig")
  }
  # remove column
  dat$sd_norm_a <- NULL
}

# round all int columns to 3 sig figs
dat$int_a <- signif(dat$int_a, 3)
dat$norm_a <- signif(dat$norm_a, 3)
dat$int_a_IS <- signif(dat$int_a_IS, 3)

# Get sampling dates ####

# If date-time info is given, use this to give each sample a time
if (!is.null(settings$zeiten_tabelle)) {
  times <- read.csv(settings$zeiten_tabelle, stringsAsFactors = F)
  stopifnot(all(c("filename", "time") %in% colnames(times)))
  # Parse date-times
  times$time <- lubridate::parse_date_time(times$time, orders = "ymdHM", 
                                           tz = "Europe/Berlin", truncated = 2)
  # Join date-times with file names
  times <- times[, c("filename", "time")]
  dat <- merge(dat, times, by.x = "samp", by.y = "filename")
  
  # get fields for date and time
  dat$start <- as.character(dat$time)  # yyyy-MM-dd HH:mm or yyyy-MM-dd
  dat$filename <- NULL
  dat$time <- NULL
# If date info is provided in the file name, extract this information  
} else if (!is.null(settings$datum_format)) {
  # the sample names must contain the sampling dates!
  dat$start <- switch(settings$datum_format,
                                ymd = {
                                  date_string <- stringr::str_match(dat$samp, settings$datum_regex$ymd)[,2]
                                  temp <- lubridate::ymd(date_string, tz = "Europe/Berlin", truncated = 0)
                                  format(temp, "%Y-%m-%d", tz = "Europe/Berlin")
                                },
                                ym = {
                                  date_string <- stringr::str_match(dat$samp, settings$datum_regex$ymd)[,2]
                                  temp <- lubridate::ymd(date_string, tz = "Europe/Berlin", truncated = 1)
                                  format(temp, "%Y-%m-%d", tz = "Europe/Berlin")
                                },
                                yy = {
                                  date_string <- stringr::str_match(dat$samp, settings$datum_regex$yy)[,2]
                                  temp <- lubridate::ymd(date_string, tz = "Europe/Berlin", truncated = 2)
                                  format(temp, "%Y-%m-%d", tz = "Europe/Berlin")
                                },
                                stop("Format not known")
  )
} else {
  stop("Could not parse dates, provide a format for dates in filenames or a table")
}

stopifnot(all(!is.na(dat$start)))

# Get CAS ####

cas <- dbas$peakList[, c("comp_CAS", "comp_name")]
cas <- cas[!duplicated(cas),]
dat <- merge(dat, cas, by = "comp_name")

# Get other metadata ####

dat$date_import <- round(as.numeric(Sys.time()))  # epoch_seconds
dat$duration <- settings$probenahmedauer

# add optional location parameters
if (!is.null(settings$probenahmestelle)) {
  stopifnot(is.element(settings$probenahmestelle, station))
  dat$station <- settings$probenahmestelle
}
  

# add required parameters
stopifnot(
  !is.null(settings$polaritaet),
  is.element(settings$polaritaet, pol),
  !is.null(settings$matrix),
  !is.null(settings$datenquelle),
  !is.null(settings$hplc_methode)
)
dat$pol <- settings$polaritaet
dat$matrix <- settings$matrix
dat$data_source <- settings$datenquelle
dat$hplc_method <- settings$hplc_methode


# change names to make it easier to type
# colnames(dat) <- gsub("samp", "probenbezeichnung", colnames(dat))
colnames(dat) <- gsub("^comp_name$", "name", colnames(dat))
colnames(dat) <- gsub("^int_a$", "area", colnames(dat))
colnames(dat) <- gsub("^int_a_IS$", "area_is", colnames(dat))
colnames(dat) <- gsub("^comp_CAS$", "cas", colnames(dat))
colnames(dat) <- gsub("^samp$", "filename", colnames(dat))
#colnames(dat) <- gsub("norm_a", "normierte_peakflaeche", colnames(dat))

# make dat into list to allow for nested data structure
rownames(dat) <- NULL
datl <- split(dat, seq_len(nrow(dat))) 
datl <- lapply(datl, as.list)

# Get coordinates ####
if (!is.null(settings$koordinaten)) {
  # add coordinates, assuming it is the same for all samples
  datl <- lapply(datl, function(feature) {
    feature$loc$lat <- settings$koordinaten$breitengrad
    feature$loc$lon <- settings$koordinaten$laengengrad
    feature
    })
} else if (!is.null(settings$koord_tabelle)) {
  # add specific coordinates for each sample
  coords <- read.csv(settings$koord_tabelle, stringsAsFactors = F)
  datl <- lapply(datl, function(feature) {
    getLat <- coords[feature$filename == coords$filename, "lat"]
    getLon <- coords[feature$filename == coords$filename, "lon"]
    stopifnot(is.numeric(getLat), is.numeric(getLon))
    feature$loc$lat <- getLat
    feature$loc$lon <- getLon
    feature
  })
} else {
  # it is required to give sample coordinates
  stop("Could not get coordinates, check settings file")
}
 
# Get river km ####
# if available
# one km
if (!is.null(settings[["fluss_km"]])) { # *$ notation does a substring match
  datl <- lapply(datl, function(feature) {
    feature$km <- settings[["fluss_km"]]
    feature
  })
}
# multiple km
if (!is.null(settings$fluss_km_tabelle)) {
  kms <- read.csv(settings$fluss_km_tabelle) 
  datl <- lapply(datl, function(feature) { # feature <- datl[[1]]
    getKm <- kms[feature$filename == kms$filename, "km"]
    if (is.na(getKm)) {
      return(feature)
    } else {
      getKm <- as.numeric(getKm)
      stopifnot(is.numeric(getKm))
      feature$km <- getKm
    }
    feature
  })
}

# Get river name ####
if (!is.null(settings[["fluss_name"]]))
  dat$river <- settings[["fluss_name"]]
# multiple river names, all samples must have a river name
if (!is.null(settings$fluss_name_tabelle)) {
  rivnames <- read.csv(settings$fluss_km_tabelle)
  datl <- lapply(datl, function(feature) {
    getRivName <- rivnames[feature$filename == rivnames$filename, "river"]
    stopifnot(length(getRivName) == 1, is.character(getRivName), nchar(getRivName) > 0)
    feature$river <- getRivName
    feature
  })
}

# Get gkz ####
if (!is.null(settings[["fluss_gkz"]]))
  dat$gkz <- settings[["fluss_gkz"]]
# multiple gkz
if (!is.null(settings$fluss_gkz_tabelle)) {
  rivGkz <- read.csv(settings$fluss_gkz_tabelle)
  datl <- lapply(datl, function(feature) { 
    getRivGkz <- rivGkz[feature$filename == rivGkz$filename, "gkz"]
    if (is.na(getRivGkz) || nchar(getRivGkz) == 0) {
      return(feature)
    } else {
      getRivGkz <- as.numeric(getRivGkz)
      stopifnot(length(getRivGkz) == 1, is.numeric(getRivGkz))
      feature$gkz <- getRivGkz
    }
    feature
  })
}

datl <- unname(datl)

message("Collecting spectra...")

# Add m/z, rt, eic, ms1 and ms2 ####
datl <- parallel::mclapply(datl, function(doc) { # doc <- datl[[1]]

  idtemp <- subset(dbas$peakList, comp_name == doc$name & samp == doc$filename, peakID, drop = T)

  if (!is.numeric(idtemp))
    stop("non numeric idtemp") 
  # if more than one peak matches (isomers), which should be marked by peak A, B etc, 
  # choose the peak with the highest intensity
  if (length(idtemp) > 1) { 
    best <- which.max(subset(dbas$peakList, peakID %in% idtemp, int_a, drop = T))
    idtemp <- subset(dbas$peakList, peakID %in% idtemp, peakID, drop = T)[best]
    doc$comment <- paste(doc$comment, "isomers found")
  }
  # mz
  mztemp <- subset(dbas$peakList, peakID == idtemp, real_mz, drop = T)
  if (length(mztemp) == 0)
    mztemp <-subset(dbas$integRes, comp_name == doc$name & samp == doc$filename, real_mz, drop = T)
  stopifnot(length(mztemp) == 1, is.numeric(mztemp), !is.na(mztemp))
  doc$mz <- round(mztemp, 4)
  
  # rt
  rttemp <- subset(dbas$peakList, peakID == idtemp, real_rt_min, drop = T)
  if (length(rttemp) == 1 && is.na(rttemp))
    rttemp <- subset(dbas$peakList, peakID == idtemp, rt_min, drop = T)
  if (length(rttemp) == 0)
    rttemp <- subset(dbas$integRes, comp_name == doc$name & samp == doc$filename, real_rt_min, drop = T)
  stopifnot(length(rttemp) == 1, is.numeric(rttemp), !is.na(rttemp))
  doc$rt <- round(rttemp, 2)
  
  # if the id was not found, then the rest of the inputs are not needed
  if (!is.numeric(idtemp) || length(idtemp) == 0)
    return(doc)
  
  # int
  inttemp <- subset(dbas$peakList, peakID == idtemp, int_h, drop = T) 
  # eic
  
  if (is.numeric(idtemp)) {
    eictemp <- subset(dbas$EIC, peakID == idtemp, c(time, int))
    if (nrow(eictemp) > 0) {
      eictemp$time <- round(eictemp$time)  # in seconds
      eictemp$int <- round(eictemp$int, 4)
      rownames(eictemp) <- NULL
      doc$eic <- eictemp
    }
    # ms1
    ms1temp <- subset(dbas$MS1, peakID == idtemp, c(mz, int))
    if (nrow(ms1temp) > 0 && is.numeric(inttemp)) {
      ms1temp <- norm_ms1(ms1temp, mztemp, inttemp)
      if (!is.null(ms1temp)) {
        ms1temp$mz <- round(ms1temp$mz, 4)
        ms1temp$int <- round(ms1temp$int, 4)
        rownames(ms1temp) <- NULL
        doc$ms1 <- ms1temp
      }
    }
    # ms2
    ms2temp <- subset(dbas$MS2, peakID == idtemp, c(mz, int))
    if (nrow(ms2temp) > 0) {
      ms2temp <- norm_ms2(ms2temp, mztemp)
      if (!is.null(ms2temp)) {
        ms2temp$mz <- round(ms2temp$mz, 4)
        ms2temp$int <- round(ms2temp$int, 4)
        rownames(ms2temp) <- NULL
        doc$ms2 <- ms2temp
      }
    }
  }
  doc
}, mc.cores = settings$cores) 


# remove NA values (there is no such thing as NA, the value just does not exist)
datl <- lapply(datl, function(doc) Filter(function(x) is.list(x) || !(is.na(x) || x == "NA"), doc))

# add tags if available
if (!is.null(settings$tag)) {
  datl <- lapply(datl, function(doc) {doc$tag <- settings$tag ; doc})
}

message("Writing JSON...")

# Generate JSON ####

jsonPath <- gsub("\\.report$", ".json", reportFile)  

jsonlite::write_json(datl, jsonPath, pretty = T, digits = NA, auto_unbox = T)

message("json saved to: ", appendLF = F)
message(jsonPath)

