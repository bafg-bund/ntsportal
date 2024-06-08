#' Get data from elastic
#'
#' @param escon Connection object created with `elastic::connect`
#' @param index Name of index must be one of "g2_nts_expn", "g2_nts_bfg"
#' @param station Can be one of "KOMO", "FAN", "WIN", "rhein_ko_l"
#' @param startRange Two element character vector with date range e.g. c("2021-01-01", "2022-01-01")
#' @param responseField Can be one of "intensity", "intensity_normalized", "area", "area_normalized"
#' @param ufidLevel Can be 1 or 2. Level 1: mz+rt+ms2, level 2: mz+rt
#' @param form Format of output table, can be one of "long", "wide"
#'
#' @return data.frame
#' @export
#'
#' @examples
#' \dontrun{
#' config_path <- "~/projects/config.yml"
#' ec <- config::get("elastic_connect", file = config_path)
#' escon <- elastic::connect(host = "10.140.73.204", user = ec$user, pwd = ec$pwd)
#' index <- "g2_nts_expn"
#' get_time_series(
#'   escon,
#'   station = "KOMO",
#'   startRange = c("2021-01-01", "2022-01-01"),
#'   responseField = "intensity"
#' )
#' }
#'
#' @import lubridate
get_time_series <- function(
    escon,
    index = "g2_nts_expn",
    station = "KOMO",
    startRange = c("2021-01-01", "2022-01-01"),
    responseField = "intensity",
    ufidLevel = 2,
    form = "long") {
  stopifnot(length(startRange) == 2)
  if (!all(grepl("^\\d{4}-\\d{2}-\\d{2}$", startRange))) {
    stop("'start_range' must be length 2 character with the format 'yyyy-mm-dd'")
  }
  stopifnot(length(responseField) == 1, is.element(responseField, c("intensity", "intensity_normalized", "area", "area_normalized")))
  stopifnot(length(station) == 1, is.element(station, c("KOMO", "FAN", "WIN", "rhein_ko_l")))
  stopifnot(length(form) == 1, is.element(form, c("long", "wide")))
  stopifnot(length(index) == 1, is.element(index, c("g2_nts_expn", "g2_nts_bfg")))
  stopifnot(inherits(escon, "Elasticsearch"))
  stopifnot(is.numeric(ufidLevel), length(ufidLevel) == 1, is.element(ufidLevel, c(1, 2)))
  ufidType <- ifelse(ufidLevel == 1, "ufid", "ufid2")
  ucidType <- ifelse(ufidLevel == 1, "ucid", "ucid2")

  # need to partition results since bucket overload may occur
  # get number of ufids

  numUfids <- elastic::Search(escon, index, body = sprintf(
    '
    {
      "query": {
        "bool": {
          "filter": [
            {
              "range": {
                "start": {
                  "gte": "%s",
                  "lte": "%s"
                }
              }
            },
            {
              "term": {
                "station": "%s"
              }
            }
          ]
        }
      },
      "size": 0,
      "aggs": {
        "ufid_num": {
          "cardinality": {
            "field": "%s"
          }
        }
      }
    }
    ', startRange[1], startRange[2], station, ufidType
  ))$aggregations$ufid_num$value

  # get number of days

  numDays <- interval(ymd(startRange[1]), ymd(startRange[2])) / ddays(1)

  # estimate number of partitions to ufids so that max buckets < 50000
  numPartitions <- ceiling(numUfids / (50000 / numDays))
  message("Request split into ", numPartitions, " partitions.")

  get_data_partition <- function(part) {
    message("Partition ", part)
    ans <- elastic::Search(escon, index, body = sprintf(
      '
    {
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "station": "%s"
              }
            },
            {
              "range": {
                "start": {
                  "gte": "%s",
                  "lte": "%s"
                }
              }
            }
          ]
        }
      },
      "size": 0,
      "aggs": {
        "ufids": {
          "terms": {
            "field": "%s",
            "include": {
               "partition": %i,
               "num_partitions": %i
            },
            "size": 10000
          },
          "aggs": {
            "mz": {
              "avg": {
                "field": "mz"
              }
            },
            "rt": {
              "avg": {
                "field": "rt"
              }
            },
            "names": {
              "terms": {
                "field": "name",
                "size": 10
              }
            },
            "polarity": {
              "terms": {
                "field": "pol",
                "size": 2
              }
            },
            "ucid": {
              "terms": {
                "field": "%s",
                "size": 10
              }
            },
            "over_time": {
              "date_histogram": {
                "field": "start",
                "calendar_interval": "day"
              },
              "aggs": {
                "durations": {
                  "terms": {
                    "field": "duration"
                  },
                  "aggs": {
                    "response": {
                      "avg": {
                        "field": "%s"
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    ', station, startRange[1], startRange[2], ufidType, part - 1, numPartitions, ucidType, responseField
    ))

    message(ans$hits$total$value, " features returned")
    ufids <- ans$aggregations$ufids$buckets
    message("Grouped into ", length(ufids), " ufids")
    df <- data.frame(
      ufid = vapply(ufids, "[[", numeric(1), i = "key"),
      mz = vapply(ufids, function(x) x$mz$value, numeric(1)),
      rt = vapply(ufids, function(x) x$rt$value, numeric(1)),
      ucid = vapply(ufids, function(x) {
        ucb <- x$ucid$buckets
        if (length(ucb) == 0) {
          return(0)
        } else if (length(ucb) == 1) {
          return(ucb[[1]]$key)
        } else {
          stop("more than one ucid for ufid ", x$key)
        }
      }, numeric(1)),
      name = vapply(ufids, function(x) {
        nb <- x$names$buckets
        if (length(nb) == 0) {
          return("")
        } else if (length(nb) == 1) {
          return(nb[[1]]$key)
        } else {
          return(paste(vapply(nb, "[[", character(1), i = "key"), collapse = ", "))
        }
      }, character(1)),
      pol = vapply(ufids, function(x) {
        nb <- x$polarity$buckets
        if (length(nb) == 0) {
          return("")
        } else if (length(nb) == 1) {
          return(nb[[1]]$key)
        } else {
          return(paste(vapply(nb, "[[", character(1), i = "key"), collapse = ", "))
        }
      }, character(1))
    )

    resp <- lapply(ufids, function(ufidBuck) {
      dayBucks <- ufidBuck$over_time$buckets
      dayRows <- lapply(dayBucks, function(dayBuck) {
        durBucks <- dayBuck$durations$buckets
        responseRows <- lapply(durBucks, function(durBuck) {
          c(
            ufid = ufidBuck$key,
            start = dayBuck$key_as_string,
            duration = durBuck$key,
            response = durBuck$response$value
          )
        })
        do.call("rbind", responseRows)
      })
      do.call("rbind", dayRows)
    })
    resp <- do.call("rbind", resp)
    resp <- as.data.frame(resp)
    resp$ufid <- as.numeric(resp$ufid)
    resp$duration <- as.numeric(resp$duration)
    resp$response <- as.numeric(resp$response)
    resp$start <- substr(resp$start, 1, 10)
    resp$start <- ymd(resp$start)
    merge(df, resp, by = "ufid")
  }

  aligTables <- lapply(seq_len(numPartitions), get_data_partition)

  # long form
  aligTable <- do.call("rbind", aligTables)

  if (ufidLevel == 2) {
    colnames(aligTable) <- sub("^ufid$", "ufid2", colnames(aligTable))
    colnames(aligTable) <- sub("^ucid$", "ucid2", colnames(aligTable))
  }

  # wide form
  if (form == "wide") {
    aligTable <- tidyr::pivot_wider(aligTable, names_from = "start", values_from = "response", values_fill = 0)
  }

  aligTable
}


#' Get entire time series for one compound at one station
#'
#' Function should be expanded to allow user to choose date range and get data
#' from more than one compound, more than one station.
#'
#' @param escon Elastic connection opject
#' @param index Index name
#' @param compound Compound name to filter data by (see ntsportal for correct spelling)
#' @param station Station name to filter data by (see available station codes in the ntsportal wiki)
#' @param pol Polarity to filter data by (either "pos" or "neg")
#' @param matrix Matrix code to filter data by (see available matrix codes in the ntsportal wiki)
#' @param calendarInterval Calendar internval, see available options in elasticsearch documentations for `date_histogram`
#'
#' @return
#' @export
#'
#' @examples
get_time_series_known <- function(escon, index = "g2_dbas_bfg", 
                         compound = "Carbamazepine", 
                         station = "rhein_ko_l", 
                         pol = "pos", matrix = "water", 
                         calendarInterval = "day") {
  
  
  result <- elastic::Search(escon, index = index, body = sprintf('
  {
    "size": 0,
    "query": {
      "bool": {
        "filter": [
          {
            "term": {
              "name": "%s"
            }
          },
          {
            "term": {
              "station": "%s"
            }
          },
          {
            "term": {
              "pol": "%s"
            }
          },
          {
            "term": {
              "matrix": "%s"
            }
          }
        ]
      }
    },
    "aggs": {
      "dates": {
        "date_histogram": {
          "field": "start",
          "calendar_interval": "%s"
        },
        "aggs": {
          "avg_norm_a": {
            "avg": {
              "field": "norm_a"
            }
          }
        }
      }
    }
  }
  ', compound, station, pol, matrix, calendarInterval))
  
  dates <- vapply(result$aggregations$dates$buckets, "[[", i = "key_as_string", character(1))
  areas <- vapply(result$aggregations$dates$buckets, function(x) {
    temp <- x$avg_norm_a$value
    if (is.null(temp)) NA else temp
  }, numeric(1))
  dates <- lubridate::ymd_hms(dates)
  data.frame(start = dates, norm_a = areas, stringsAsFactors = F)
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

