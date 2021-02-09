[Back to main page](../README.md)

# Economy
Information related to the economic development for each region.


## URL
This table can be found at the following URLs depending on the choice of format:
* [economy.csv](https://storage.googleapis.com/covid19-open-data/v2/economy.csv)
* [economy.json](https://storage.googleapis.com/covid19-open-data/v2/economy.json)


## Schema
| Name | Name | Description | Example |
| ---- | ---- | ----------- | ------- |
| **key** | `string` | Unique string identifying the region | CN_HB |
| **gdp** | `integer` `[USD]` | Gross domestic product; monetary value of all finished goods and services | 24450604878 |
| **gdp_per_capita** | `integer` `[USD]` | Gross domestic product divided by total population | 1148 |
| **human_capital_index** | `double` `[0-1]` | Mobilization of the economic and professional potential of citizens | 0.765 |


## Sources of data

<details>
<summary>Show data sources</summary>


| Data | Source | License and Terms of Use |
| ---- | ------ | ------------------------ |
| Metadata | [Wikipedia](https://wikidata.org) | [Terms of Use][24] |
| Metadata | [Eurostat](https://ec.europa.eu/eurostat) | [CC BY][33] |
| Economy | [Wikidata](https://wikidata.org) | [CC0][23] |
| Economy | [DataCommons](https://datacommons.org) | [Attribution required](https://policies.google.com/terms) |
| Economy | [WorldBank](https://worldbank.org) | [CC BY](https://www.worldbank.org/en/about/legal/terms-of-use-for-datasets) |

</details>


[7]: https://github.com/GoogleCloudPlatform/covid-19-open-data/blob/main/examples/data_loading.ipynb
[12]: https://open-covid-19.github.io/explorer
[13]: https://kepler.gl/demo/map?mapUrl=https://dl.dropboxusercontent.com/s/cofdctuogawgaru/COVID-19_Dataset.json
[14]: https://www.starlords3k.com/covid19.php
[15]: https://kiksu.net/covid-19/
[18]: https://www.bsg.ox.ac.uk/research/research-projects/oxford-covid-19-government-response-tracker
[19]: https://auditter.info/covid-timeline
[20]: https://www.coronavirusdailytracker.info/
[21]: https://omnimodel.com/
[22]: https://console.cloud.google.com/marketplace/product/bigquery-public-datasets/covid19-open-data
[23]: https://www.wikidata.org/wiki/Wikidata:Licensing
[24]: https://foundation.wikimedia.org/wiki/Terms_of_Use
[28]: https://data.humdata.org/about/license
[29]: http://creativecommons.org/licenses/by/4.0/
[30]: https://reproduction.live/
[31]: http://opendefinition.org/licenses/odc-by/
[32]: https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
[33]: https://ec.europa.eu/info/legal-notice_en#copyright-notice
