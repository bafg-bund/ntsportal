

# ntsportal 25.2

## Major changes

* `dbaScreening*()` now save records as RDS files rather than compressed JSON files (necessary due to licence 
  compatibility issues). RDS files for ingest must contain `ntsportal-featureRecord` in the name.
* Added new fields to `dbas` tables: `score_ms2_match` which gives the ms² match score (0-1000). This is used in the 
  "Spectra of Annotated Features" dashboard to show a) the number of MS² matches in a dataset and b) the distribution of
  MS² matching scores (histogram); `esi_ion_spec` (runtime field) which is the concatenation of `name`, `pol`, `adduct` and `isotopologue`
* Sample metadata is added to `dbas` documents during ingest (enrich policy in ingest pipeline, similar to a join). This
  is to reduce redundancy in output RDS files. See `inst/enrichPolicies` and `inst/ingestPipelines`. The user is 
  required to update the enrich policies prior to ingest with `updateEnrichPolicies()`. Several functions were added to
  manage the enrich policies and ingest pipelines needed for ntsportal.
* Fixed bug whereby peak areas were overwritten with peak intensities
* Added mapping type `nondetect_dbas` which is used to store non-detects in nondetect_dbas tables
* ntsportal is now freely available on Github under the GPL-3.0 licence (https://github.com/bafg-bund/ntsportal)

## Minor changes

* Added batch consistency checks to `checkMsrawfiles()`
* Added functions for manipulating array type fields in documents
* Updated to use CSL v25.5 and ntsworkflow 0.2.9


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
