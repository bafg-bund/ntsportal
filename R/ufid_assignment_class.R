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
