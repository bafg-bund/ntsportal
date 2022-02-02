library(lubridate)
library(ggplot2)
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)
escon$ping()

df <- ntsportal::get_time_series(
  escon,
  index = "g2_nts_bfg",
  station = "rhein_ko_l",
  startRange = c("2021-01-01", "2021-11-10"),
  responseField = "intensity"
)

format(object.size(df), units = "MB", standard = "SI")

df <- ntsportal::get_time_series(
  escon,
  index = "g2_nts_expn",
  station = "KOMO",
  startRange = c("2021-01-01", "2022-01-01"),
  responseField = "intensity"
)

df2 <- subset(df, ufid == 47)
df2[df2$response == 0, "response"] <- NA
ggplot(df2, aes(ymd(start), response)) + geom_line()
