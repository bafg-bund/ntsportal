
<!-- README.md is generated from README.Rmd. Please edit README.Rmd -->

# ntsportal

The goal of ntsportal is to provide a database and online dashboard for
non-target-screening (based on LC/GC-HRMS measurements) of surface
waters. The system is designed to archive processed data and to carry
out simple data analysis in online dashboards. For detailed statistical
analysis, the API can be used for accessing the data in scripts. The
system currently uses
[Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
as the DBMS. This R-package is used for processing data and building the
database. The included scripts for automated data processing use
[ntsworkflow](https://github.com/bafg-bund/ntsworkflow). Latest
information on the structure and internal design of ntsportal is found
in the wiki.

For German government agencies, access to the internally managed
NTSPortal database is available. Please contact the authors or the
Federal Institute of Hydrology. This repository includes the back-end
code for open-source development purposes but does not include the
environmental data.

## Project Funding

The project was initiated by a research funding grant by the Federal
Environment Agency (UBA, “Online Portal: Non-Target-Screening für die
Umweltüberwachung der Zukunft” REFOPLAN 3720222010) and continued
funding is provided the research grant REFOPLAN 3723222020
(“Weiterentwicklung des Online Portals für die Umweltbeobactung der
Zukunft”). Additional funding is provided by the Federal Ministry of
Digital and Transport and the Federal Ministry of the Environment and
Consumer Protection.

## License

Copyright 2025 Bundesanstalt für Gewässerkunde (Federal Institute of
Hydrology)

ntsportal is free software: you can redistribute it and/or modify it
under the terms of the GNU General Public License as published by the
Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

ntsportal is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for
more details.

You should have received a copy of the GNU General Public License along
with ntsportal. If not, see <https://www.gnu.org/licenses/>.

If you use the package or any derivative thereof, please cite the work.
To cite this work please use the following citation or run
`citation("ntsportal")`.

> Kevin S. Jewell, Ole Lessmann, Franziska Thron, Jonas Skottnik, Iris
> Tuchscherer, Arne Wick and Thomas A. Ternes (2025). ntsportal: A
> Non-Target Screening Data Archive and Distribution Tool. R package
> version 0.2.1.

## Installation

### Current installation guidance for pvil-rr (temporary)

1)  Create a gitlab PAT
2)  Add it, with the following line, to `~/.Rprofile`

``` r
Sys.setenv(GITLAB_PAT = "<...>")
```

3)  Restart R session
4)  Install ntsworkflow and ntsportal

``` r
devtools::install_gitlab(
  repo = "nts/ntsworkflow",
  host = "https://gitlab.lan.bafg.de"
)

devtools::install_gitlab(
  repo = "nts/ntsportal",
  host = "https://gitlab.lan.bafg.de"
)
```

5)  Install python requirements

``` r
reticulate::virtualenv_install(requirements = fs::path_package("ntsportal", "pythonElasticComm", "requirements.txt"))
```
