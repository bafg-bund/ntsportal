help_ui <- function(id){
  fluidPage(
  # todo cannot open file 'R/_Help/include/include.mdinclude.html': No such file or directory
    titlePanel("includeText, includeHTML, and includeMarkdown"),
  
    fluidRow(
      column(4,
             includeText(paste0("R/_Help/include/","include.txt")),
             br(),
             pre(includeText(paste0("R/_Help/include/","include.txt")))
      ),
      column(4,
           includeHTML(paste0("R/_Help/include/","include.html"))
      ),
      column(4,
             includeMarkdown(paste0("R/_Help/include/","include.md"))
      )
    )
  )
}