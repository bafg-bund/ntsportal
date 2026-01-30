
# ntsportal 25.3

## Major changes (backend)

* Changed table names of screening results tables from `ntsp25.2_dbas*` to `ntsp25.3_feature*`. These tables include
both DBAS (library screening) and NTS (unknown screening) processing results.
* New processing method "NTS" to record unknown features (no CSL annotation). Unknown features are added to `feature`
tables. Unknown features are currently not aligned/grouped, i.e., there is no `name` field. This will be added in a 
future version.
* Multiple library screening hits currently produce duplicate features with different entries in the fields with 
compound annotation metadata (`name`, `cas`, `adduct`, etc.). These duplicate features are now linked by the `multi_hit_id` 
field (duplicate features have the same ID), which allows easier removal of duplicate features in data processing scripts.
* The compound annotation metadata (`name`, `cas`, `adduct`, etc.) is added to the `compound_annotation` table (nested field).
This allows multiple annotations per feature in the case of multiple library screening hits. The duplication of multi-hit
features (see preceding point) is currently still necessary for search in the Kibana front-end. In a future version, the
duplication will be removed and the top-level compound annotation fields (`name`, etc.) will be deprecated in favor 
of the nested fields (`compound_annotation.name`, etc.). 
* For the search API client in R, replaced `getTableAsTibble()` with `getTableByQuery()` and `getTableByEsql()`. 
Both return a `tbl_df`, AKA "tibble" (an extension of `data.frame`). The former uses the *Query DSL* format 
(passed as a `list`). The latter allows the user to send ES|QL statements (as text) for queries and computing 
statistics, see the "Discover" page at `https://ntsportal.bafg.de` for more information.
* The documentation website is now [online](https://docs.ntsportal.bafg.de/).
* Two new articles "Document field descriptions for NTSPortal" and "Processing by non-target screening" were added to
the documentation.
* Open data licence changed to CC BY 4.0 for all BfG-measured data.


## Major changes (frontend)

* Additions for better interpretation of the confidence in detections
  + "MS² available" annotation on time series 
  + Distribution of MS² similarity scores in "Spectra of annotated features" dashboard
* The results of the NTS processing workflow can be viewed in the "Unknowns overview" dashboard
* Time series now include notes (annotations) to indicate unavoidable changes in analytical or processing methods

## Minor changes (backend)

* Changed field type to `integer`, `half_float` or `scaled_float` for better data compression. Modified fields: 
`area`, `intensity`, `area_internal_standard`, `intensity_internal_standard`, `rt`, `eic.int`, `eic.time`, `ms1.int`, 
`ms2.int`
* Changed `duration` field to `keyword`. The field now uses ISO 8601 time duration format, see 
[documentation.](https://docs.digi.com/resources/documentation/digidocs/90001488-13/reference/r_iso_8601_duration_format.htm)
* Changed name of `ingest()` to `ingestFeatureRecords()` since this function only has this specific purpose. All other 
ingesting is done by `appendRecords()`
* Bug correction: Features found only once in replicate samples (as determined by `replicate_regex`) are removed. The 
annotation level (1 or 2 [gap-filled]) does not play a role.
* Vignette documentation moved from vignettes to pkgdown articles, which are available on the documentation website 
(to reduce installation size).
* Field names in `msrawfiles` documents have changed to accommodate NTS processing.
  - `dbas_blank_regex` to `blank_regex`
  - `dbas_date_format` to `start_date_format`
  - `dbas_date_regex` to `start_date_regex`
  - `dbas_is_name` to `internal_standard`
  - `dbas_replicate_regex` to `replicate_regex`
  - `dbas_instr` to `csl_instruments_allowed`
  - `dbas_spectral_library` to `spectral_library_path`
* Fields removed from `msrawfiles`
  - `dbas_use_area`
  - `nts_spectral_library_sha256`
  - `dbas_mustFindChromPeak`
  - `dbas_build_averages`
  - `dbas_cores_report`
  - `dbas_cores`
  - `nts_spectral_library`
  - `dbas_spectral_library_sha256`
  - `nts_index_name`
  - `nts_alias_name`
  - `dbas_alias_name`
  - `nts_alig_mz_tol_units`
* Field names in `spectral_library` documents have changed
  - `frag_type` to `collision_type`
* Updated various dependencies to versions:
  + R = 4.5.2
  + Python = 3.12.12
  + ElasticSearch = 9.2.3
  + elasticsearch (Python) = 9.2.1
  + CSL = 25.7
  + ntsworkflow = 0.2.10
  
  
  
# ntsportal 25.2

## Major changes (backend and frontend)

* Added new fields to `dbas` tables: `score_ms2_match` which gives the ms² match score (0-1000). This is used in the 
  "Spectra of Annotated Features" dashboard to show a) the number of MS² matches in a dataset and b) the distribution of
  MS² matching scores (histogram); `esi_ion_spec` (runtime field) which is the concatenation of `name`, `pol`, `adduct` 
  and `isotopologue`
* Fixed bug whereby peak areas were overwritten with peak intensities
* ntsportal is now available on Github under the GPL-3.0 licence (https://github.com/bafg-bund/ntsportal)

## Major changes (only backend)

* `dbaScreening*()` now save records as RDS files rather than compressed JSON files (necessary due to licence 
  compatibility issues). RDS files for ingest must contain `ntsportal-featureRecord` in the name.
* Sample metadata is added to `dbas` documents during ingest (enrich policy in ingest pipeline, similar to a join). This
  is to reduce redundancy in output RDS files. See `inst/enrichPolicies` and `inst/ingestPipelines`. The user is 
  required to update the enrich policies prior to ingest with `updateEnrichPolicies()`. Several functions were added to
  manage the enrich policies and ingest pipelines needed for ntsportal.
* Added mapping type `nondetect_dbas` which is used to store not detected compounds in nondetect_dbas tables

## Minor changes

* Added batch consistency checks to `checkMsrawfiles()`
* Added functions for manipulating array type fields in documents
* Updated to use the [Collective Spectral Library (CSL) 25.5](https://doi.org/10.5281/zenodo.16901590) and 
  [ntsworkflow](https://github.com/bafg-bund/ntsworkflow) 0.2.9 


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
