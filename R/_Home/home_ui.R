home_ui <- function(id){
  fluidPage(
  # todo cannot open file 'R/_Help/include/include.mdinclude.html': No such file or directory

    includeHTML(paste0("R/_Home/include/","home.html"))
  )
}