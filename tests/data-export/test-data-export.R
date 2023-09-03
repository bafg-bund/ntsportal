

# test data export
source("~/connect-ntsp.R")

resp <- ntsportal::get_time_series(
     escon, 
     station = "KOMO",
     startRange = c("2021-01-01", "2022-01-01"),
     responseField = "intensity"
   )


write.csv(resp, "tests/data-export/example-data-export.csv")

