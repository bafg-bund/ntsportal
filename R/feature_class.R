#' Create feature object
#' 
#' @description Creates a feature object which holds the information needed to define one feature
#'
#' @param pol polarity of the feature
#' @param mz mass of the feature
#' @param rt retentiontime of the feature
#' @param es_id
#' @param chrom_method which method was used to measure
#' @param ...
#'
#' @details A feature is a list with the same fields as the schema defined in the NTSPortal DBAS or NTS databases. 
#' @returns object of class ntsportal::feature
#' @export
#'
new_feature <- function(pol = character(), mz = double(), rt = double(),
                        es_id = character(), chrom_method = character(), ...) {
  stopifnot(
    is.numeric(mz), is.numeric(rt), is.character(es_id),
    is.character(pol), is.character(chrom_method)
  )
  # if there are no decimal places, mz automatically parsed to integer
  if (is.integer(mz)) {
    mz <- as.double(mz)
  }
  x <- list(mz = mz, rt = rt, pol = pol, es_id = es_id, chrom_method = chrom_method)
  furtherArgs <- list(...)
  okfields <- c("ms1", "ms2", "rtt", "eic", "filename", "data_source", 
                "date_import", "intensity", "ufid")
  stopifnot(all(names(furtherArgs) %in% okfields))

  for (newField in names(furtherArgs)) {
    x[[newField]] <- furtherArgs[[newField]]
  }

  structure(x, class = "feature")
}

#' Check feature 
#'
#' @param x Feature object
#' 
#' @details This function will through an error if checks fail.
#' @returns Unchanged feature object if checks have passed
#' @export
#'
validate_feature <- function(x) {
  if (is.null(x$mz) || is.na(x$mz) || length(x$mz) == 0) {
    stop("Must have an mz")
  }
  stopifnot(length(x$pol) == 1, x$pol %in% c("pos", "neg"))
  x
}

# Methods for feature class ####

#' @export
print.feature <- function(feat) {
  cat(sprintf("feature: %.4f@%.2f %s; ", feat$mz, feat$rt, feat$pol))
  cat(sprintf("fields: %s", paste(names(feat), collapse = ", ")))
}
