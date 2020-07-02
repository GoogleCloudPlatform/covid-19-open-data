# Cache
This directory contains very simple code used to cache data sources related to COVID-19. The main
purpose is to download data that is updated daily and potentially overwritten. In order to ensure
reliability, the logic is kept as simple as possible.

## Config
The list of data sources is maintained in the [`config.json`](config.json) file. It contains a list
of records with the following fields:

| Name | Description | Example |
| ---- | ----------- | ------- |
| url | The URL of the resource that should be downloaded / crawled | https://www.cdph.ca.gov/Programs/CID/DCDC/Pages/COVID-19/Race-Ethnicity.aspx |
| cmd | Command that will process the URL and download a cached copy | `static_download` |
| output | Name of the output file which will be written under the current snapshot | US_CA-mortality-stratified.html |
| `option` | Some commands use additional options, which can be defined here | `{screenshot: true}` |

## Running
The caching is run automatically by the CI server. If you wish to run it locally, first install
the dependencies by running this command from the `src` directory:
```sh
# `npm install` would also work
yarn
```

Then, execute the following script from the `src` directory as well:
```sh
python ./cache.py
```

The output will be placed under the `../output/cache/` directory.

## License
Each data source cached here will retain its original license. The [LICENSE file](../../LICENSE)
only applies to the code and data owned and published by this repository.
