library(ntsportal)
library(ggplot2)
library(lubridate)
index <- "g2_nts_expn"
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
config_path <- "config.yml"
path_ufid_db <- "~/HRMS_Z/sw_entwicklung/ntsportal/ufid1.sqlite"
#config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)

escon <- elastic::connect(host = 'elastic-mn-01.hpc.bafg.de', port = 9200, user=ec$user, pwd=ec$pwd, transport_schema = "https")
#escon$ping()

alig <- get_time_series(escon, index = "g2_nts_expn",
                station = "KOMO",
                startRange = c("2021-01-01", "2022-01-01"),
                responseField = "intensity",
                ufidLevel = 2,
                form = "long")

# linegraph all data
ggplot(subset(test, name=="Benzotriazole"), aes(start,response, group =1))+
  geom_line()+
  theme(axis.text.x = element_text(angle = 90))

# example benzotriazole
ggplot() +
  geom_line(aes(start, response), subset(alig, ufid2 == 77 & duration == 1)) +
  geom_step(aes(start, response), subset(alig, ufid2 == 77 & duration != 1), color = "blue")

ggplot() +
  geom_step(aes(start, response), subset(alig, ufid2 == 77))

# go through alig table and for every composite sample, replicate the row for the number of days
expandedRows <- lapply(seq_len(nrow(alig)), function(i) { # i <- 5
  dur <- alig[i, "duration"]
  if (dur == 1)
    return(NULL)
  startDate <- alig[i, "start"]
  toAdd <- alig[rep(i, dur+1), ]
  toAdd$start <- startDate + seq(0, dur, 1)
  toAdd$duration <- 1
  toAdd
})
expandedRows <- Filter(Negate(is.null), expandedRows)
expandedRows <- do.call("rbind", expandedRows)
expandedRows$expanded <- TRUE
alig <- subset(alig, duration == 1)
alig$expanded <- FALSE
alig <- rbind(alig, expandedRows)

# if there is a daily sample on that day, need to ignore the mixed sample
alig2 <- by(alig, list(alig$start, alig$ufid2), function(x) {
 if (nrow(x) == 1) {
   x
 } else if (any(!x$expanded)) {
   x <- x[!x$expanded, ]
   avgResp <- mean(x$response)
   x <- x[1,]
   x$response <- avgResp
   x
 } else {
   avgResp <- mean(x$response)
   x <- x[1,]
   x$response <- avgResp
   x
 }
}, simplify = F)
alig2 <- do.call("rbind", alig2)
View(subset(alig2, ufid2 == 77))
interv <- ymd(20210401) %--% ymd(20210601)
alig3 <- alig2[alig2$ufid2 == 77 & sapply(alig2$start, function(x) x %within% interv) , ]
alig3 <- alig2[alig2$ufid2 == 77, ]
ggplot() + geom_line(aes(start, response), alig3)





