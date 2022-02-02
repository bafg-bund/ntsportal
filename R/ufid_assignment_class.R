

# ufid_assignment class functions ####

#' New ufid
#'
#' @param x
#' @param level
#'
#' @return
#' @export
#'
#' @examples
new_ufid_assignment <- function(x = integer(), level = integer()) {
  stopifnot(is.integer(x), is.integer(level))
  structure(x, class = "ufid_assignment", level = level)
}


#' Check ufid
#'
#' @param feat
#'
#' @return
#' @export
#'
#' @examples
is_ufid_assignment <- function(feat) {
  all("level" %in% names(attributes(feat)),
      is.integer(feat),
      is.integer(attr(feat, "level")))
}

#' Print ufid
#'
#' @param x
#'
#' @return
#' @export
#'
#' @examples
print.ufid_assignment <- function(x) {
  cat("ufid=", unclass(x), " level=", attr(x, "level"))
}
