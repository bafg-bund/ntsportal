#' Create feature
#'
#' @param pol
#' @param mz
#' @param rt
#' @param es_id
#' @param chrom_method
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
new_feature <- function(pol = character(), mz = double(), rt = double(),
                        es_id = character(), chrom_method = character(), ...) {
  stopifnot(is.numeric(mz), is.numeric(rt), is.character(es_id),
            is.character(pol), is.character(chrom_method))
  # if there are no decimal places, mz automatically parsed to integer
  if (is.integer(mz))
    mz <- as.double(mz)
  x <- list(mz = mz, rt = rt, pol = pol, es_id = es_id, chrom_method = chrom_method)
  furtherArgs <- list(...)
  okfields <- c("ms1", "ms2", "rtt", "eic", "filename", "data_source", "date_import", "intensity")
  stopifnot(all(names(furtherArgs) %in% okfields))
  
  for (newField in names(furtherArgs)) {
    x[[newField]] <- furtherArgs[[newField]]
  }
  
  structure(x, class = "feature")
}

#' Check
#'
#' @param x
#'
#' @return
#' @export
#'
#' @examples
validate_feature <- function(x) {
  if (is.null(x$mz) || is.na(x$mz) || length(x$mz) == 0)
    stop("Must have an mz")
  if ("ufid" %in% names(x))
    stopifnot(is_ufid_assignment(x$ufid))
  stopifnot(length(x$pol) == 1, x$pol %in% c("pos", "neg"))
  x
}

#' @export
print.feature <- function(feat) {
  cat(sprintf("feature: %.4f@%.2f %s; ", feat$mz, feat$rt, feat$pol))
  cat(sprintf("fields: %s", paste(names(feat), collapse = ", ")))
}
