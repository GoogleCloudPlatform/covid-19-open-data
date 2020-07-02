# Changelog

### Open COVID-19 V2
Preview of V2 finalized:
* Audited all data sources
* Improved reliability of data
* Implemented new data ingestion pipeline
* Changed column names to snake_case to improve readability
* Added capability for 3 levels of region hierarchy (country, state/province and county)
* Broke down tables into: metadata, epidemiology, demographics, geography, economy, stringency
* Matched every region with a wikidata ID
* Added tested and recovered data to epidemiology table
* Added county-level data for CO, GB, NL and US
* Now tracking both new and cumulative cases
