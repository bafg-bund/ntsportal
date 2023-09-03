

# data from envimass must be cleaned to fit current status of ntsportal
# this is okay, because the nts

# load data
fn <- commandArgs(trailingOnly = T)[1]
#fn <- "json_data/target_screening_pos.json"
dt <- jsonlite::read_json(fn)

#View(dt[[1]])
# remove date_export 
dt <- lapply(dt, function(doc) {doc$date_export <- NULL; doc})
# change date to start
dt <- lapply(dt, function(doc) {doc$start <- doc$date; doc$date <- NULL; doc})

# change station name
#dt <- lapply(dt, function(doc) {doc$station <- "rhein_ko_l"; doc})

# copy area to norm_a
dt <- lapply(dt, function(doc) {doc$norm_a <- doc$area; doc})

# change method to chrom_method
dt <- lapply(dt, function(doc) {doc$chrom_method <- doc$method; doc$method <- NULL; doc})

# change adduct nomenclature
dt <- lapply(dt, function(doc) {
  if ("adduct" %in% names(doc)) {
    doc$adduct <- sub("1-$", "-", doc$adduct)
    doc$adduct <- sub("1\\+$", "+", doc$adduct)
    if (doc$adduct == "[M+]+")
      doc$adduct <- "[M]+"
  }
  doc
})

# remove any NULL values
dt <- lapply(dt, function(doc) Filter(Negate(is.null), doc))

# remove ms2 "none detected"
dt <- lapply(dt, function(doc) {
  if ("ms2" %in% names(doc) && 
      length(doc$ms2) == 1 && 
      (doc$ms2 == "none detected" || doc$ms2 == "no MS2 scans")
  )
    doc$ms2 <- NULL
  doc
})

# change gkz to number

dt <- lapply(dt, function(doc) {doc$gkz <- as.integer(doc$gkz); doc})

# change eic.time to integer

dt <- lapply(dt, function(doc) {
  if ("eic" %in% names(doc)) {
    doc$eic <- lapply(doc$eic, function(x) {x$time <- round(x$time); x})
  }
  doc
})

# make new filename
newfn <- sub("\\.json$", "_edit.json", fn)
message("writing new file")
jsonlite::write_json(dt, newfn, pretty = T, digits = NA, auto_unbox = T)

message("cleaning complete.")
