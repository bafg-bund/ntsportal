# ntsportal 25.1

* Initial Github publication

## Major changes

* Temporary removal of processing and indexing of unknown features (`ntsp_nts` indices) until stable release with `ntsp_dbas` 
indices is achieved.
* Introduced a unified versioning system for back-end, front-end and database content based on the `ntsp25.1` form.
Index naming now takes the form `ntsp25.1_dbas_XXX`. Likewise for the front end (Kibana space ID: `ntsportal-2025-1`).
* Moved most DB communication to an S4-based interface class and implemented methods using 
the official python *elasticsearch* client via *reticulate*.
* Agglomeration of all documentation and wikis into *pkgdown* site.
* Refactoring of much of the code and a large expansion of tests.
