histogramUI <- function(id, df) {
  tagList(
    selectInput(NS(id, "var"), "Variable", names(df)),
    numericInput(NS(id, "bins"), "bins", 10, min = 1),
    plotOutput(NS(id, "hist"))
  )
}
