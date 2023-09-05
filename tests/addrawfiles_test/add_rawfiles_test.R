source("~/connect-ntsp.R")
rfindex <- "g2_msrawfiles"
templateId <- find_templateid(escon, rfindex, polarity = "neg", station = "saale_wettin_m")

newPath <- "~/Messdaten/00_to_sort/20230831_SPM/pos/Wett_08_2_pos.mzXML"

debug(add_rawfiles)
isdebugged(add_rawfiles)
undebug(add_rawfiles)

add_rawfiles(escon, rfindex, templateId, newPath)


debug(find_templateid)
undebug(find_templateid)

