
# ntsportal-viewer

ntsportal-viewer is an app that connects to elastic, retrieves data and visualizes it. The end user also has the option of downloading the data.



### Project structure

    .
    ├── app.R                                         # main script, to start the app
    ├── R                 
    │   ├── _Dashboard                                # dashboard module
    │   │   ├── dashboard_server.R                    # scripts for the dashboard tab
    │   │   ├── dashboard_ui.R       
    │   │   ├── gomap.js  
    │   │   └── styles.css
    │   ├── _Data                                     # data module
    │   │   ├── data_server.R                         
    │   │   └── data_ui.R   
    │   ├── _Help                                     # help module
    │   │   ├── help_server.R
    │   │   ├── hlep_ui.R  
    │   │   └── include                               # area where you can create 
    │   │       ├── include.html                      # and manage the help page
    │   │       ├── include.md
    │   │       └── include.txt
    │   ├── _Home                                     # home module
    │   │   ├── home_server.R
    │   │   ├── home_ui.R  
    │   │   └── include                               # area where you can create 
    │   │       ├── home.html                         # and manage the home page
    │   │       ├── bfg-bruecke.jpg
    │   │       └── bfg-graphik.png
    │   ├── _Request                                  # request module
    │   │   ├── request_server.R                      
    │   │   └── request_ui.R  
    │   ├── _Test                                     # not active module, only for testing
    │   │   ├── request_server_old.R                  # old functions
    │   │   ├── request_ui_old.R                      # can be deleted when project
    │   │   ├── test_leaflet.R                        # is ready
    │   │   ├── test_req.R       
    │   │   ├── test_server.R 
    │   │   └── test_ui.R
    │   ├── global.R                                  # global functions + settings
    │   ├── server.R                                  # main server script for manageing the modules 
    │   └── ui.R                                      # main ui script
    ├── src                                           # help functions / source code
    │   ├── data_preprocessing                        # data preprocessing functions
    │   │   └── dashboard_preprocessing               # prepare data for visualization
    │   │       └── dashboard_preprocessing_data.R    
    │   └── elastic                                   # elastic specific function 
    │       ├── elastic_connection                    
    │       │   └── elastic_connection.R              # create a connectionto elastic
    │       └── elastic_queries 
    │           ├── get_data_from_elastic.R           # these scripts are responsible
    │           ├── list_all_indices_elastic.R        # for the data query from elastic
    │           ├── query_filter_func_elastic.R 
    │           └── query_template.txt 
    ├── Data                                          # test / demo datasets
    │   ├── cbz_cand.json
    │   └── example-dataset-structure.json
    ├── Docker                                        # docker / docker-compose files
    │   ├── .gitignore
    │   ├── docker-compose.yml
    │   └── Dockerfile
    ├── renv                                          # environment specific files
    │   ├── .gitignore
    │   ├── activate.R       
    │   ├── settings.json  
    │   ├── library      
    │   ├── library_root
    │   └── staging
    ├── renv.lock
    ├── .gitignore
    ├── .Renviron
    ├── .Rprofile
    ├── LICENSE
    ├── README.md
    └── README.html


### Connection config
`config.yml`

```ruby
bafg_elastic_connect:
  user: "user"
  pwd: "pwd"
  host: "host"
  port: "port"
  transport_schema: "https"
  authorization: "ApiKey"
```
