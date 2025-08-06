

# ntsportal 25.2

## Major changes

* `dbaScreening*()` now save records as RDS files rather than compressed JSON files (necessary due to licence 
  compatibility issues). RDS files for ingest must contain `ntsportal-featureRecord` in the name.
* Added new field to `dbas` tables: `score_ms2_match` which gives the ms² match score (0-1000). This is used in the 
  "Spectra of Annotated Features" dashboard to show a) the number of MS² matches in a dataset and b) the distribution of
  MS² matching scores (histogram)
* Sample metadata is added to `dbas` documents during ingest (enrich policy in ingest pipeline, similar to a join). This
  is to reduce redundancy in output RDS files. See `inst/enrichPolicies` and `inst/ingestPipelines`. The user is 
  required to update the enrich policies prior to ingest with `updateEnrichPolicies()`
* Fixed bug whereby peak areas were overwritten with peak intensities
* Updated to use CSL v25.5 and ntsworkflow 0.2.9

## Minor changes

* Added batch consistency checks to `checkMsrawfiles()`
* Added functions for manipulating array type fields in documents

# ntsportal 25.1

## Major changes

* Temporary removal of processing and indexing of unknown features (`ntsp_nts` indices) until stable release with `ntsp_dbas` 
indices is achieved.
* Introduced a unified versioning system for back-end, front-end and database content based on the `ntsp25.1` form.
Index naming now takes the form `ntsp25.<X>_dbas_<code>`. Likewise for the front end (Kibana space ID: `ntsportal-2025-1`).
* Moved most DB communication to an S4-based interface class and implemented methods using 
the official python *elasticsearch* client via *reticulate*.
* Agglomeration of all documentation and wikis into *pkgdown* site.
* Refactoring of much of the code and a large expansion of tests.
