
public <- "docs/"
message("Document")
devtools::document(".")

message("URL checks")
urlchecker::url_check(".")
urlchecker::url_update(".")

message("Build Vignettes")
devtools::build_vignettes(".", clean = FALSE, install = FALSE)
tools::compactPDF(paths = "doc", gs_quality = "ebook")

message("Build website")
pkgdown::build_site(".", preview = FALSE, new_process = TRUE)

# Make changes to html files
files <- list.files(path = public, pattern = "*[.]html",
                    all.files = TRUE, full.names = FALSE, recursive = TRUE,
                    ignore.case = FALSE, include.dirs = TRUE, no.. = FALSE)

for (a_file in files) {
  
  x <- readLines(paste0(public, a_file))
  
  # Add imprint to footer
  y <- gsub('Developed by Kevin S. Jewell, Ole Lessmann.',
            paste0('Developed by Kevin S. Jewell, Ole Lessmann, Jonas Skottnik, Franziska Prodöhl, Nina Hermes, Arne Wick. <a href="https',
                   '://www.bafg.de/EN/Service/Imprint/imprint_node.html"',
                   '>Imprint</a>.'), x)
  
  # remove the prefix "technical report" in references
  z <- gsub('Technical Report ', '', y)
  
  # export html
  cat(z, file = paste0(public, a_file), sep="\n")
}

# clean up
rm(a_file, files, x, y, z)
bfgdown::cleanAll(public, "http://r.bafg.de/")
bfgdown::insertLogo(public, "pkgdown/bfg_logo.png", href = "https://www.bafg.de", text = "BfG")

# sync with public_html
host <- Sys.info()["nodename"]
user <- Sys.info()["user"]
if (host == "pvil-rr.bafg.de" & user == "Jewell") {
  system(sprintf("cp -rp %s/* /home/%s/public_html/ntsportal_pkgdown", public, user))
}