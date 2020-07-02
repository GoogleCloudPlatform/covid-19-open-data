# Epidemiology
This table is probably the most important one in this dataset. Because of that, additional
instructions are provided for how to add new data sources. A canonical example is
[this commit](https://github.com/open-covid-19/data/commit/0d1d978ce9c6afaecbf5fc9ac9fb386f77d16d4f)
which adds a new data source to for epidemiology data broken down into subregions of Afghanistan.


## Step 0: Identify a data source
Before adding new data, we must have a data source. The data source must be able to produce
historical data. If the data source you found only has the last day's data, then consider adding it
to the cache first; then aggregate the cache entries into a historical view of the data source. See
the [cache documentation](../../cache/README.md) for more details.


## Step 1: Populate metadata table
Only records which have a corresponding key in the [metadata.csv](../../data/metadata.csv) table
will be ingested. When the pipeline is run, you will see output similar to this for records which do
not have a corresponding entry in the metadata table:
```
.../lib/pipeline.py:158: UserWarning: No key match found for:
match_string        La Guaira
date               2020-04-16
total_confirmed            14
new_confirmed             NaN
country_code               VE
_vec                La Guaira
Name: 0, dtype: object
```

This indicates that a record with the `match_string` value of "La Guaira" was not matched with any
entries in the metadata table. Sometimes there simply isn't a good match; in this case "La Guaira"
is a city which does not have a good region correspondence -- subregion1 is state/province and
subregion2 is county/municipality.

Most countries use ISO 3166-2 codes to report epidemiology data at the subregion level. If that's
the case for the data source you are trying to add, then you can look for the subregions declared
in the [iso_3166_2_codes.csv](../../data/iso_3166_2_codes.csv) table and copy/paste them into the
metadata table (don't forget to reorder the columns and add missing fields, since metadata.csv has
more columns than iso_3166_2_codes.csv).

For extra credit, make sure that there is also a corresponding entry in the
[knowledge_graph.csv](../../data/knowledge_graph.csv) table for all the keys added into
metadata.csv.


## Step 2: Add an entry to the config YAML
Each pipeline has a `config.yaml` configuration file that determines which parsing scripts and in
what order they are run. The configuration file also includes the URL(s) of the raw resources that
will be downloaded prior to processing. Here's an example for the configuration snippet which adds
a parsing script for Afghanistan regional data:
```yaml
sources:

  # Full class name of the parsing script which subclasses `DataSource`, relative to `./src`
  - name: pipelines.epidemiology.af_humdata.AfghanistanHumdataDataSource
    fetch:
      # `fetch` contains a list of URLs which will be downloaded and passed to the `parse` function
      - url: "https://docs.google.com/spreadsheets/d/1F-AMEDtqK78EA6LYME2oOsWQsgJi4CT3V_G4Uo-47Rg/export?format=csv&gid=1539509351"
        opts:
          # If the extension is not obvious from the URL, you can force a file extension like this
          ext: csv
    # Options can be passed to the parsing script like this
    parse:
      opt_name: "opt_value"
```

This data source has a single URL, but you can have as many as necessary and a list will be provided
to the `parse` function.


## Step 3: Create a parsing script for your source
A file with the path provided in the `name` of the configuration in step #2 will contain the parsing
script. It should contain a class with a unique name descriptive of the data source and subclassing
`DataSource`. If the data source is an authoritative one (i.e. government or health ministry) then
the file should be named `xx_authority.py` where `xx` is the 2-character country code. If the data
is being downloaded from any source other than the authority directly, then name it
`xx_sourcename.py` where `sourcename` is some short descriptive name derived from the location that
the data is being pulled from.

If the source is in CSV, JSON, XLS or XLSX format, then you can override the `parse_dataframes`
method and receive a pandas DataFrame for each URL defined in the fetch of the YAML config (see step
#2). Otherwise, you should override the `parse` which will provide you with a list of files in the
local filesystem.

Here's an example of a very simple parsing script:
```python
class MySourceNameDataSource(DataSource):
    def parse_dataframes(
        self, dataframes: List[DataFrame], aux: Dict[str, DataFrame], **parse_opts
    ) -> DataFrame:

        # Data is only one source, so we only look at the first item of the list
        data = (
            dataframes[0]
            # Rename the columns to the appropriate names according to the schema
            .rename(
                columns={
                    "Date": "date",
                    "Province": "match_string",
                    "Cases": "new_confirmed",
                    "Deaths": "new_deceased",
                    "Active Cases": "current_confirmed",
                    "Recoveries": "new_recovered",
                }
            )
        )

        # It's not infrequent for subregion names to be duplicate in different countries, it's also
        # good for performance reasons to narrow down the potential matches as much as possible so
        # we can declare the country code for all records coming from this dataset
        data["country_code"] = "AF"

        # Here we most likely need to do additional processing, but we return as-is as an example
        # For instance, we should probably compute `total_confirmed`, `total_deceased` and
        # `total_recovered` by performing the cumsum of each of those columns grouped by key.
        return data
```

The arguments to the parsing script are:
* `dataframes`: list of URLs downloaded and parsed using the appropriate `pandas.read_*` function
* `aux`: dictionary of auxiliary dataframes that might be helpful during processing, defined by the
  `auxiliary` option in the `config.yaml` file and normally containing files from the
  [data folder](../../data).
* `parse_opts`: options passed to this script via the `config.yaml` configuration. In the example
  from step #2, here we would receive {opt_name: opt_value}.

The core idea is that you need to write a script overriding the `DataSource` class which
implements a `parse` method and outputs a set of variables (confirmed cases, deaths, tests, etc.)
alongside whatever information is needed to match each record to a `key` and `date`. The output is
in a Pandas DataFrame, and each record may look like this:
`{key: US_CA, date: 2020-04-04, new_confirmed: 13, total_confirmed: 1134, ... }`

If you have a `key` for each record, then that's the best case scenario since there's no opportunity
for ambiguity. In the example above, the US_CA key corresponds to country `US` and state `CA`. Then
the data ingestion pipeline takes the record and matches it against this metadata.csv table.

Most data sources, unfortunately, have data in formats that make it difficult to derive the record
key. In many cases, they don't even provide any sort of code identifier for the region and only a
name or label is given. As you can imagine, the names for regions do not have canonical values and
can sometimes be in the local language. Then, your best hope is to output as much information as you
can to ensure there is one (and only one) match with records from the metadata.csv table. If `key`
is not available, then a combination of `country_code`, `country_name`, `subregion1_code` and/or
`subregion1_name` is preferred.

If all else fails, there is a special column called `match_string` which will attempt to match what
you provide as value against `subregion1_name`, `subregion1_code`, `subregion1_name`,
`subregion2_code` and, if all else fails, attempt a regex match against `match_string` from
metadata.csv. In practice, `country_code` + `match_string` is fine in 99% of cases unless the number
of records in the data source is very large, in which case you should try to build the key by any
means possible for performance reasons.


## Step 4: Test your script
Once the scraping script is finished, the easiest way to test it is to comment out all the other
pipeline configurations from `config.yaml` and inspect the console output as well as the resulting
table output at [the output folder](../../../output/tables). To run the epidemiology pipeline,
execute the following command from the `src` folder:
```sh
python update.py --only epidemiology --verify simple
```

The goal should be to find a match for **every** record in the source dataset. Sometimes, that's
not possible (see step #1 for an example). Rather than cluttering the console output, you should try
remove the offending records before returning a DataFrame in your parsing script.
