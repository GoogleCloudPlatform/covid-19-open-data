# DeepMind/Google COVID-19 Data for England

Contact
[dm_c19_modelling@google.com](mailto:dm_c19_modelling@google.com?subject=[C19%20England%20Data])
for comments and questions.

This toolkit is for processing COVID-19 data for England and merging with Google
Cloud's
[COVID-19 Open Data Repository](https://github.com/GoogleCloudPlatform/covid-19-open-data).

There is a variety of COVID-19 data published online for England, some of which
has been aggregated and incorporated into the COVID-19 Open Data Repository.
This toolkit adds several data sources not previously captured there. These
include:

*   `daily_deaths`: Deaths associated with COVID-19 in England, reported by
    individual NHS trust. This data is found
    [here](https://www.england.nhs.uk/statistics/statistical-work-areas/covid-19-daily-deaths).
    The download link varies, and matches:

    -   URL root:
        `https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/<regex-below>`
    -   Regex:
        `([0-9]{4}/[0-1][0-9]/COVID-19-total-announced-deaths-[0-9]{1,2}-\w*-[0-9]{4}\.xlsx)`
        The stored filename is `daily_deaths.xlsx` (see below for absolute
        URLs).

*   `daily_cases`: Confirmed cases of COVID-19 in England reported by lower-tier
    local authority (LTLA). This data is found
    [here](https://coronavirus.data.gov.uk). The download link is
    [here](https://coronavirus.data.gov.uk/downloads/csv/coronavirus-cases_latest.csv)
    The stored filename is `daily_cases.csv` (see below for absolute URLs).

*   `online_111`: Numbers of online 111 queries which are associated with
    possible COVID-19 symptoms, reported by England clinical commissioning group
    (CCG). This data is found
    [here](https://digital.nhs.uk/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest).
    The download link varies, and matches:

    -   URL root: `https://files.digital.nhs.uk/<regex-below>`
    -   Regex:
        `(.+/.+/111%20Online%20Covid-19%20data_[0-9]{4}-[0-1][0-9]-[0-9]{2}\.csv)`
        The stored filename is `online_111.csv` (see below for absolute URLs).

*   `calls_111_999`: Numbers of 111 and 999 phone calls which are associated
    with possible COVID-19 symptoms, reported by England CCG. This data is found
    [here](https://digital.nhs.uk/data-and-information/publications/statistical/mi-potential-covid-19-symptoms-reported-through-nhs-pathways-and-111-online/latest).
    The download link varies, and matches:

    -   URL root: `https://files.digital.nhs.uk/<regex-below>`
    -   Regex:
        `(.+/.+/NHS%20Pathways%20Covid-19%20data%[0-9]{4}-[0-1][0-9]-[0-9]{2}\.csv)`
        The stored filename is `calls_111_999.csv` (see below for absolute
        URLs).

*   `population`: Population counts, reported by CCG, for 2019. This data is
    found
    [here](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/clinicalcommissioninggroupmidyearpopulationestimates).
    The download link is
    [here](https://www.ons.gov.uk/file?uri=%2fpeoplepopulationandcommunity%2fpopulationandmigration%2fpopulationestimates%2fdatasets%2fclinicalcommissioninggroupmidyearpopulationestimates%2fmid2019sape22dt6a/sape22dt6amid2019ccg2020estimatesunformatted.zip).
    The stored filename is
    `SAPE22DT6a-mid-2019-ccg-2020-estimates-unformatted.xlsx` (see below for
    absolute URLs).

## Pipeline

### Raw data sources

The raw data sources are automatically scraped daily and stored in their raw
formats in a public location,
`https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/<YYYYMMDD>/<filename>`,
where `<YYYYMMDD>` is the scrape date and `<filename>` is the stored filename
listed above for each data source. For example, the "daily cases" for 19 April
2021 are:
[https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/daily_cases.csv](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/20210419/daily_cases.csv).

The raw population data is stored here:
[https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/SAPE22DT6a-mid-2019-ccg-2020-estimates-unformatted.xlsx](https://storage.cloud.google.com/covid19-open-data/dm-c19-data/raw/SAPE22DT6a-mid-2019-ccg-2020-estimates-unformatted.xlsx)
(The standardized and mergeable population data is stored similar to the other
data as described below.)

The YAML file `dm_c19_modelling/england_data/filename_regexes.yaml` contains a
mapping from the labels indicated above to a Python regex for matching the raw
data's filename.

### Standardizing the raw data

The Python module `standardize_data.py` contains tools for processing the raw
data into a standardized format, with common date and region labels. The script
`run_standardize_data.py` can be run to read the raw data, standardize it, and
write the standardized version to disk.

For example, if `<root-dir>/raw` is the location of the raw data described
above:

`$ run_standardize_data.py --raw_data_directory="<root-dir>/raw"
--output_directory="<root-dir>/standardized" --scrape_date="20210115"`

These are stored daily in,
`https://storage.cloud.google.com/covid19-open-data/dm-c19-data/standardized/<YYYYMMDD>/...`,
analogous to the raw data.

### Formatting the standardized data so it can be merged with Google Cloud's [COVID-19 Open Data Repository](https://github.com/GoogleCloudPlatform/covid-19-open-data)

The Python module `dataset_merge_util.py` contains tools for processing the
standardized data into a format which can be merged directly with the COVID-19
Open Data Repository, in particular by constructing and incorporating the
necessary region keys. The script `run_dataset_merge.py` can be run to read the
standardized data, format it for merging, and write the mergeable version to
disk.

For example, if `<root-dir>/standardized` is the location of the standardized
data:

`$ run_dataset_merge.py --input_directory="<root-dir>/standardized"
--output_directory="<root-dir>/mergeable" --scrape_date="20210115"`

These are stored daily in:
`https://storage.cloud.google.com/covid19-open-data/dm-c19-data/mergeable/<YYYYMMDD>/...`,
analogous to the raw data.


**Disclaimer**
This is not an official Google product.
