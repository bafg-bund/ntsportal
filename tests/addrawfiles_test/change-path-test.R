
source("~/connect-ntsp.R")
rfindex <- "g2_msrawfiles"

pths <- list.files("~/HRMS_Z/NTS_Sachsen/02_Messdaten/20220201_NTS_Sachsen_19/pos/mzXML/", f = T)
pths
npths <- list.files("~/messdaten/sachsen/pos/paket19/", f = T)
npths

pths <- data.frame(path = pths, fname = basename(pths))
npths <- data.frame(path = npths, fname = basename(npths))
df <- merge(pths, npths, by = "fname", suffixes = c(".old", ".new"))

debug(change_msrawfile_path)
undebug(change_msrawfile_path)
change_msrawfile_path(escon, rfindex, df[2, "path.old"], df[2, "path.new"])
