
# ntsportal-viewer

Shiny App to view ntsportal elasticsearch database

Wie ist die beste (Standard-)Struktur eines R-Skripts? Auf welche Dinge
sollten wir achten?
<https://www.r-bloggers.com/2018/08/structuring-r-projects/>

.

└── my_awesome_project

  ├── src

  ├── output

  ├── man (if roxygen)

  ├── renv

  ├── data

  │ ├── raw

  │ └── processed

  ├── README.md

  ├── run_analyses.R

  ├── renv.lock

  ├── .Rprofile

  └── .gitignore



'''ruby
bafg_elastic_connect:
  user: "user"
  pwd: "pwd"
  host: "host"
  port: "port"
  transport_schema: "https"
  authorization: "ApiKey"
'''